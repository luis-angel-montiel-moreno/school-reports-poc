from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count, avg, when, lit
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder \
    .appName("SchoolDistrictDW-MLAnalytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

base_path = "/tmp/delta"
ml_path = f"{base_path}/ml"

# Load base tables
users = spark.read.format("delta").load(f"{base_path}/users")
courses = spark.read.format("delta").load(f"{base_path}/courses")
grades = spark.read.format("delta").load(f"{base_path}/grades")
homeworks = spark.read.format("delta").load(f"{base_path}/homeworks")
adjustments = spark.read.format("delta").load(f"{base_path}/adjustments")

# Map letter grades to GPA for numeric modeling
from pyspark.sql.functions import expr
letter_to_gpa = expr(
    "CASE grade "
    "WHEN 'A+' THEN 4.0 WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7 "
    "WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7 "
    "WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 WHEN 'C-' THEN 1.7 "
    "WHEN 'D+' THEN 1.3 WHEN 'D' THEN 1.0 WHEN 'F' THEN 0.0 "
    "ELSE NULL END"
)

# Build per-student features
students = users.where(col("role") == "student").select("id", "name", "school_id")

student_course_counts = grades.select("student_id", "course_id").distinct() \
    .groupBy("student_id").agg(countDistinct("course_id").alias("num_courses"))

student_hw_counts = homeworks.groupBy("student_id").agg(count("id").alias("num_homeworks"))

student_avg_gpa = grades.withColumn("gpa", letter_to_gpa) \
    .groupBy("student_id").agg(avg("gpa").alias("avg_gpa"))

student_changes = adjustments.groupBy("student_id").agg(count("id").alias("num_changes"))

features_df = students \
    .join(student_course_counts, students.id == student_course_counts.student_id, "left") \
    .join(student_hw_counts, students.id == student_hw_counts.student_id, "left") \
    .join(student_avg_gpa, students.id == student_avg_gpa.student_id, "left") \
    .join(student_changes, students.id == student_changes.student_id, "left") \
    .select(
        students.id.alias("student_id"),
        "name", "school_id",
        col("num_courses").cast(DoubleType()),
        col("num_homeworks").cast(DoubleType()),
        col("avg_gpa").cast(DoubleType()),
        col("num_changes").cast(DoubleType())
    ) \
    .fillna({"num_courses": 0.0, "num_homeworks": 0.0, "avg_gpa": 0.0, "num_changes": 0.0})

# Save features
features_df.write.format("delta").mode("overwrite").save(f"{ml_path}/student_features")

# KMeans clustering on student features
assembler = VectorAssembler(
    inputCols=["num_courses", "num_homeworks", "avg_gpa", "num_changes"],
    outputCol="features_raw"
)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

kmeans = KMeans(k=4, seed=42, featuresCol="features", predictionCol="cluster")

kmeans_pipeline = Pipeline(stages=[assembler, scaler, kmeans])

kmeans_model = kmeans_pipeline.fit(features_df)
clustered = kmeans_model.transform(features_df)

# Save clustering assignments
clustered.select("student_id", "name", "school_id", "num_courses", "num_homeworks", "avg_gpa", "num_changes", "cluster") \
    .write.format("delta").mode("overwrite").save(f"{ml_path}/student_clusters")

# Decision Tree classification: predict High vs Low performer
# Label: high_performer = 1 if avg_gpa >= 3.0 else 0
labeled = features_df.withColumn("label", when(col("avg_gpa") >= lit(3.0), lit(1.0)).otherwise(lit(0.0)))

dt_assembler = VectorAssembler(
    inputCols=["num_courses", "num_homeworks", "num_changes"],
    outputCol="dt_features_raw"
)
dt_scaler = StandardScaler(inputCol="dt_features_raw", outputCol="features", withMean=True, withStd=True)

# No train/test split due to small sample size; fit and evaluate
classifier = DecisionTreeClassifier(labelCol="label", featuresCol="features", predictionCol="prediction", maxDepth=3, seed=42)

clf_pipeline = Pipeline(stages=[dt_assembler, dt_scaler, classifier])
clf_model = clf_pipeline.fit(labeled)

predictions = clf_model.transform(labeled)

# Evaluate
acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy").evaluate(predictions)
precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions)
recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall").evaluate(predictions)

# Confusion matrix
from pyspark.sql.functions import concat_ws
cm = predictions.groupBy("label", "prediction").count()

# Save outputs
predictions.select("student_id", "name", "avg_gpa", "num_courses", "num_homeworks", "num_changes", "label", "prediction") \
    .write.format("delta").mode("overwrite").save(f"{ml_path}/dt_predictions")

cm.write.format("delta").mode("overwrite").save(f"{ml_path}/dt_confusion_matrix")

# Save metrics as a small Delta table
metrics_df = spark.createDataFrame([
    (float(acc), float(precision), float(recall))
], ["accuracy", "weighted_precision", "weighted_recall"])
metrics_df.write.format("delta").mode("overwrite").save(f"{ml_path}/dt_metrics")

# Save decision tree structure for visualization
from pyspark.ml.classification import DecisionTreeClassificationModel
for stage in clf_model.stages:
    if isinstance(stage, DecisionTreeClassificationModel):
        debug_str = stage.toDebugString
        break
else:
    debug_str = "Decision tree model not found in pipeline."

debug_df = spark.createDataFrame([(debug_str,)], ["debug_string"])
debug_df.write.format("delta").mode("overwrite").save(f"{ml_path}/dt_tree_debug")

print("ML analytics completed: features, clusters, decision tree predictions, metrics, tree debug saved.")
