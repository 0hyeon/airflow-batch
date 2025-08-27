# -*- coding: utf-8 -*-
"""
feeds_transform_sharded.py
- --start-idx ~ --end-idx 범위 파일만 읽어 처리
- 필터/중복제거/결정적 컷(최대 40만) 후 TSV+gzip으로 S3 저장
"""
import argparse
from typing import List

from pyspark.sql import SparkSession, functions as F, Window


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--input-prefix", required=True)  # ex) feeds/google/gmarket
    p.add_argument("--start-idx", type=int, required=True)
    p.add_argument("--end-idx", type=int, required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--max-records", type=int, default=400000)
    p.add_argument("--dedupe-key", default="")  # "itemId,ordNo"
    p.add_argument("--target-files", type=int, default=8)
    return p.parse_args()


def list_paths(bucket: str, prefix: str, start_idx: int, end_idx: int) -> List[str]:
    paths = []
    for i in range(start_idx, end_idx + 1):
        name = f"feed_{i:05d}.tsv.gz"
        paths.append(f"s3a://{bucket}/{prefix}/{name}")
    return paths


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("feeds-transform-sharded")
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
        )
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    # 1) 대상 파일만 읽기 (TSV 헤더/압축)
    paths = list_paths(args.bucket, args.input_prefix, args.start_idx, args.end_idx)
    df = (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("multiLine", True)
        .csv(paths)
    )

    # 2) 필터링 (업무 규칙에 맞게 보완하세요)
    if "price" in df.columns:
        df = df.filter(F.col("price").isNotNull() & (F.col("price") > 0))

    # 3) 중복 제거(옵션)
    if args.dedupe_key.strip():
        keys = [k.strip() for k in args.dedupe_key.split(",") if k.strip()]
        if keys:
            w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(
                *[F.col(k) for k in keys]
            )
            df = (
                df.withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

    # 4) 결정적 컷(재현성): 전체 컬럼 해시 → 정렬 → limit
    df = (
        df.withColumn("_ord", F.xxhash64(*df.columns))
        .orderBy(F.col("_ord"))
        .limit(args.max_records)
        .drop("_ord")
    )

    # 5) 출력 (TSV + gzip)
    (
        df.coalesce(args.target_files)
        .write.mode("overwrite")
        .option("header", True)
        .option("sep", "\t")
        .option("compression", "gzip")
        .csv(args.output)
    )

    # 6) 간단 메트릭 (JSON)
    row_cnt = df.count()
    spark.createDataFrame([(row_cnt,)], ["row_count"]).coalesce(1).write.mode(
        "overwrite"
    ).json(args.output.rstrip("/") + "_metrics")

    spark.stop()


if __name__ == "__main__":
    main()
