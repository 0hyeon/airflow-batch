# -*- coding: utf-8 -*-
"""
feeds_transform_sharded.py
- mode=shard : start~end 범위의 feed_XXXXX.tsv.gz(존재하는 것만) 읽기
               → 필터(price>0) / 키 중복 제거 / 결정적 컷(40만)
               → TSV+gzip 저장 (Spark .csv.gz를 .tsv.gz로 S3 rename+메타 교체)
- mode=final : shards/ 이하 *.tsv.gz 모두 읽기
               → 같은 규칙으로 '전역 40만 cap'
               → 최종 경로 TSV+gzip 저장(+rename)
"""

import argparse
import uuid
from typing import List, Tuple

import boto3
from pyspark.sql import SparkSession, functions as F, Window


# ---------- Argparse ----------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["shard", "final"], required=True)
    p.add_argument("--bucket", required=True)  # 입력/출력 버킷
    p.add_argument(
        "--input-prefix", required=True
    )  # shard: feeds/google/<market> / final: feeds/<market>/shards
    p.add_argument("--output", required=True)  # s3://bucket/path/
    p.add_argument("--max-records", type=int, default=400000)
    p.add_argument("--dedupe-key", default="")  # 예: "itemId,ordNo"
    p.add_argument("--target-files", type=int, default=8)
    # shard 전용
    p.add_argument("--start-idx", type=int)
    p.add_argument("--end-idx", type=int)
    return p.parse_args()


# ---------- S3 Helpers ----------
def parse_s3_uri(uri: str) -> Tuple[str, str]:
    assert uri.startswith("s3://"), f"URI must start with s3://, got {uri}"
    tmp = uri[5:]
    bucket = tmp.split("/")[0]
    key_prefix = tmp[len(bucket) + 1 :] if len(tmp) > len(bucket) else ""
    if key_prefix and not key_prefix.endswith("/"):
        key_prefix += "/"
    return bucket, key_prefix


def list_keys(bucket: str, prefix: str) -> List[str]:
    s3 = boto3.client("s3")
    token = None
    keys: List[str] = []
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        keys.extend([c["Key"] for c in resp.get("Contents", [])])
        token = resp.get("NextContinuationToken")
        if not token:
            break
    return keys


def rename_csv_to_tsv_and_cleanup(tmp_out: str, final_out: str):
    """
    tmp_out: s3://bucket/path_tmp_uuid
    final_out: s3://bucket/path/   (디렉터리 prefix)
    - tmp_out 내 .csv.gz → final_out 내 .tsv.gz 로 copy
    - ContentType/Encoding 메타데이터 교체
    - _SUCCESS도 그대로 복사
    - tmp_out 전체 삭제
    """
    out_bucket, out_prefix = parse_s3_uri(final_out)
    tmp_bucket, tmp_prefix = parse_s3_uri(tmp_out)
    s3 = boto3.client("s3")

    # Copy 단계
    token = None
    while True:
        kwargs = {"Bucket": tmp_bucket, "Prefix": tmp_prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".csv.gz"):
                new_key = k.replace(tmp_prefix, out_prefix).replace(
                    ".csv.gz", ".tsv.gz"
                )
                s3.copy_object(
                    Bucket=out_bucket,
                    CopySource={"Bucket": tmp_bucket, "Key": k},
                    Key=new_key,
                    MetadataDirective="REPLACE",
                    ContentType="text/tab-separated-values",
                    ContentEncoding="gzip",
                )
            elif k.endswith("_SUCCESS"):
                new_key = k.replace(tmp_prefix, out_prefix)
                s3.copy_object(
                    Bucket=out_bucket,
                    CopySource={"Bucket": tmp_bucket, "Key": k},
                    Key=new_key,
                )

        token = resp.get("NextContinuationToken")
        if not token:
            break

    # tmp 삭제 (페이지당 최대 1000개)
    token = None
    while True:
        kwargs = {"Bucket": tmp_bucket, "Prefix": tmp_prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        dels = [{"Key": c["Key"]} for c in resp.get("Contents", [])]
        if dels:
            s3.delete_objects(Bucket=tmp_bucket, Delete={"Objects": dels})
        token = resp.get("NextContinuationToken")
        if not token:
            break


def write_tsv_gzip(df, output_uri: str, target_files: int):
    # output_uri와 같은 버킷/접두어를 쓰는 임시 경로
    tmp_out = output_uri.rstrip("/") + f"_tmp_{uuid.uuid4()}"
    (
        df.coalesce(target_files)
        .write.mode("overwrite")
        .option("header", True)
        .option("sep", "\t")
        .option("compression", "gzip")
        .csv(tmp_out)
    )
    rename_csv_to_tsv_and_cleanup(tmp_out, output_uri)


# ---------- Transform ----------
def common_transform(df, dedupe_key: str, max_records: int):
    # 가격 필터(있을 때만)
    if "price" in df.columns:
        df = df.filter(F.col("price").isNotNull() & (F.col("price") > 0))

    # 키 중복 제거(안정적)
    if dedupe_key.strip():
        keys = [k.strip() for k in dedupe_key.split(",") if k.strip()]
        # 존재하지 않는 키 방지
        keys = [k for k in keys if k in df.columns]
        if keys:
            ord_col = F.xxhash64(*df.columns)  # 전체 컬럼 기반 안정적 우선순위
            w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(ord_col.asc())
            df = (
                df.withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

    # 전역 결정적 컷: 전체 컬럼 해시로 정렬 후 limit
    df = (
        df.withColumn("_ord", F.xxhash64(*df.columns))
        .orderBy(F.col("_ord").asc())
        .limit(max_records)
        .drop("_ord")
    )
    return df


# ---------- main ----------
def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName(f"feeds-transform-{args.mode}")
        # EMRFS(기본) 사용. 커미터는 EMR 기본 커미터로 충분.
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
        )
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    if args.mode == "shard":
        if args.start_idx is None or args.end_idx is None:
            raise ValueError("--start-idx/--end-idx required in shard mode")

        # 존재하는 입력만 선택
        all_keys = set(list_keys(args.bucket, f"{args.input_prefix}/"))
        paths: List[str] = []
        for i in range(args.start_idx, args.end_idx + 1):
            k = f"{args.input_prefix}/feed_{i:05d}.tsv.gz"
            if k in all_keys:
                paths.append(f"s3://{args.bucket}/{k}")
        if not paths:
            raise FileNotFoundError(
                f"No inputs under s3://{args.bucket}/{args.input_prefix}/ "
                f"for range {args.start_idx}-{args.end_idx}"
            )

        df = (
            spark.read.option("header", True)
            .option("sep", "\t")
            .option("quote", "\u0000")  # 따옴표 무효화(성능/정확성)
            .option("escape", "\\")
            .option("multiLine", True)  # 필드 내 개행이 없으면 False로 바꿔도 OK
            .csv(paths)
        )

        df = common_transform(df, args.dedupe_key, args.max_records)
        write_tsv_gzip(df, args.output, args.target_files)

    elif args.mode == "final":
        # e.g., feeds/<market>/shards/
        keys = list_keys(args.bucket, f"{args.input_prefix}/")
        tsv_keys = [k for k in keys if k.endswith(".tsv.gz")]
        if not tsv_keys:
            raise FileNotFoundError(
                f"No shard outputs under s3://{args.bucket}/{args.input_prefix}/"
            )

        paths = [f"s3://{args.bucket}/{k}" for k in tsv_keys]
        df = (
            spark.read.option("header", True)
            .option("sep", "\t")
            .option("quote", "\u0000")
            .option("escape", "\\")
            .option("multiLine", True)  # 필요시 False
            .csv(paths)
        )

        df = common_transform(df, args.dedupe_key, args.max_records)
        write_tsv_gzip(df, args.output, args.target_files)

    spark.stop()


if __name__ == "__main__":
    main()
