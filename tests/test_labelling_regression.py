import pytest
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType

from howde import HoWDe_labelling


# ---------------------- Spark fixture ----------------------
@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for the entire test session."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("HoWDe regression tests")
        .getOrCreate()
    )
    # keep shuffles small for local tests
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    yield spark
    spark.stop()


# ---------------------- Helpers copied from your script ----------------------
def evaluate_userlevel_accuracy(truelabels_u, detectlocs_u, target_label: str, it_cols: list = []):
    """
    Evaluate user-level detection accuracy for Home or Work labels.
    A match is counted if any detected location matches the annotated true location
    for the same user at any time.
    """

    # 1) join & Boolean flags
    joined = (
        truelabels_u.alias("t")
        .join(detectlocs_u.alias("d"), ["useruuid", "loc"], "left")
        .select(
            *it_cols,
            "useruuid",
            "loc",
            (F.col("t.true_location_type") == target_label).cast("int").alias("true_f"),
            (F.col("d.location_type") == target_label).cast("int").alias("det_f"),
        )
    )

    # 2) one row per (iter, user, loc) keeping any detection of the label
    uloc = (
        joined.groupBy(*it_cols, "useruuid", "loc")
        .agg(
            F.max("true_f").alias("true_f"),
            F.max("det_f").alias("det_f"),
        )
    )

    # 3) user-level roll-up
    per_user = (
        uloc.groupBy(*it_cols, "useruuid")
        .agg(
            F.max("true_f").alias("has_true"),         # user is annotated for the label
            F.max("det_f").alias("has_detect"),        # user ever detected the label
            F.sum("true_f").alias("annot_rows"),
            F.sum(
                F.when((F.col("true_f") == 1) & (F.col("det_f") == 1), 1)
                .otherwise(0)
            ).alias("match_rows"),
        )
        .filter("has_true = 1")                        # only annotated users
    )

    # 4) final metrics
    agg = (
        per_user.groupBy(*it_cols)
        .agg(
            F.count("*").alias("count_u"),
            F.sum("has_detect").alias("wdetec_u"),
            F.sum(
                F.when(F.col("has_detect") == 1, F.col("annot_rows"))
                .otherwise(0)
            ).alias("total_rows"),
            F.sum(
                F.when(F.col("has_detect") == 1, F.col("match_rows"))
                .otherwise(0)
            ).alias("match_sum"),
        )
        .withColumn("acc", F.col("match_sum") / F.col("total_rows"))
        .withColumn("none", 1 - F.col("wdetec_u") / F.col("count_u"))
    )

    sel = it_cols + [
        f"count_u        as count_{target_label}",
        f"match_sum      as match_sum_{target_label}",
        f"wdetec_u       as detected_{target_label}",
        f"ROUND(acc*100, 2)  as acc_{target_label}",
        f"ROUND(none*100,2) as none_{target_label}",
    ]
    return agg.selectExpr(*sel)


def schemas_equivalent(
    s1: StructType,
    s2: StructType,
    ignore_nullable: bool = True,
    ignore_metadata: bool = True,
) -> bool:
    """Check if two Spark schemas are equivalent under chosen tolerances."""
    if len(s1) != len(s2):
        return False

    for f1, f2 in zip(s1.fields, s2.fields):
        if f1.name != f2.name:
            return False
        if f1.dataType != f2.dataType:
            return False
        if not ignore_nullable and f1.nullable != f2.nullable:
            return False
        if not ignore_metadata and f1.metadata != f2.metadata:
            return False
    return True


def dataframes_equal_unordered(df1, df2):
    """
    Compare two DataFrames ignoring row order, keeping duplicates.
    Column names must match as a set.
    """
    # Same columns?
    if set(df1.columns) != set(df2.columns):
        return False

    # Align column order
    cols = sorted(df1.columns)
    df1a = df1.select(*cols)
    df2a = df2.select(*cols)

    return (
        df1a.exceptAll(df2a).count() == 0
        and df2a.exceptAll(df1a).count() == 0
    )


def assert_dataframes_equivalent(df_ref, df_test, msg=""):
    same_schema = schemas_equivalent(df_ref.schema, df_test.schema)
    same_data = dataframes_equal_unordered(df_ref, df_test)

    if not same_schema or not same_data:
        schema_info = (
            f"\nRef schema: {df_ref.schema}\n"
            f"Test schema: {df_test.schema}"
        )
        raise AssertionError(
            f"DataFrames are not equivalent. "
            f"same_schema={same_schema}, same_data={same_data}. "
            f"{msg}{schema_info}"
        )


# ---------------------- The regression test ----------------------
@pytest.mark.slow
def test_userlevel_home_work_accuracy_regression(spark):
    """
    Regression test: new HoWDe_labelling output vs stored reference results.
    Uses small parquet datasets stored under tests/data.
    """

    # Paths relative to this test file
    test_dir = Path(__file__).parent
    data_dir = test_dir / "data"

    input_path = data_dir / "input" / "WB_shared_data_no_country"
    ref_H_path = data_dir / "reference" / "res_u_H"
    ref_W_path = data_dir / "reference" / "res_u_W"

    # 1) load input data
    input_data = spark.read.parquet(str(input_path))

    # 2) true labels at user-loc level
    truelabels_u = (
        input_data
        .select("useruuid", "loc", "true_location_type")
        .dropDuplicates()
    )

    # 3) run HoWDe labelling
    labeled_df = (
        HoWDe_labelling(
            input_data,
            verbose=True,
        )
        .select("useruuid", "loc", "location_type")
        .dropDuplicates()
        .cache()
    )

    # 4) compute metrics for Home & Work
    res_u_H = evaluate_userlevel_accuracy(truelabels_u, labeled_df, target_label="H")
    res_u_W = evaluate_userlevel_accuracy(truelabels_u, labeled_df, target_label="W")

    # 5) load reference outputs
    # (header option is irrelevant for parquet but harmless; kept for consistency)
    ref_res_u_H = spark.read.option("header", "true").parquet(str(ref_H_path))
    ref_res_u_W = spark.read.option("header", "true").parquet(str(ref_W_path))

    # 6) assert equivalence
    assert_dataframes_equivalent(ref_res_u_H, res_u_H, msg="Home (H) results differ.")
    assert_dataframes_equivalent(ref_res_u_W, res_u_W, msg="Work (W) results differ.")
