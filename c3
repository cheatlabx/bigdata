from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col

# Create Spark session
spark = SparkSession.builder.appName("UniversityAnalysis").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read CSV file
df = spark.read.csv(
    "student_feedback.csv",
    header=True,
    inferSchema=True
)

# Department-wise average rating
print("Department-wise Average Rating")
df.groupBy("Department").agg(
    avg("Rating").alias("Avg_Rating")
).show()

# Course-wise statistics
print("Course-wise Average Rating")
course_stats = df.groupBy("Course").agg(
    avg("Rating").alias("Avg_Rating"),
    count("Rating").alias("Feedback_Count")
)

course_stats.show()

# Show ratings greater than 4
print("High Rated Feedback")
df.filter(
    col("Rating") > 4
).show()

# Pivot table
print("Pivot Table")
df.groupBy("Department").pivot(
    "Rating"
).count().na.fill(0).show()

# Find best course
top_course = course_stats.orderBy(
    col("Avg_Rating").desc()
).first()

# Find low rated course
poor_course = course_stats.orderBy(
    col("Avg_Rating").asc()
).first()

# Display insights
print("Top Course:", top_course["Course"])
print("Course Needing Improvement:", poor_course["Course"])

# Stop Spark
spark.stop()
