from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import DeltaTable
import uuid
from datetime import datetime

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("SchoolDistrictDW") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Common columns
def now():
    return datetime.now()

def gen_id():
    return str(uuid.uuid4())

common_cols = [
    StructField("id", StringType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("metadata", StringType(), True)
]

# User Table
user_schema = StructType([
    *common_cols,
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("role", StringType(), False),
    StructField("school_id", StringType(), False)
])

# School Table
school_schema = StructType([
    *common_cols,
    StructField("name", StringType(), False),
    StructField("address", StringType(), True)
])

# Course Table
course_schema = StructType([
    *common_cols,
    StructField("name", StringType(), False),
    StructField("teacher_id", StringType(), False),
    StructField("school_id", StringType(), False)
])

# Content Table
content_schema = StructType([
    *common_cols,
    StructField("course_id", StringType(), False),
    StructField("teacher_id", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("file_type", StringType(), False)
])

# Homework Table
homework_schema = StructType([
    *common_cols,
    StructField("course_id", StringType(), False),
    StructField("student_id", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("file_type", StringType(), False)
])

# Grade Table
grade_schema = StructType([
    *common_cols,
    StructField("course_id", StringType(), False),
    StructField("student_id", StringType(), False),
    StructField("grade", StringType(), False),
    StructField("comments", StringType(), True)
])

# CourseLoadAdjustment Table
adjustment_schema = StructType([
    *common_cols,
    StructField("student_id", StringType(), False),
    StructField("course_id", StringType(), False),
    StructField("action", StringType(), False)
])

# Paths for Delta tables
base_path = "/tmp/delta"
paths = {
    "users": f"{base_path}/users",
    "schools": f"{base_path}/schools",
    "courses": f"{base_path}/courses",
    "contents": f"{base_path}/contents",
    "homeworks": f"{base_path}/homeworks",
    "grades": f"{base_path}/grades",
    "adjustments": f"{base_path}/adjustments"
}

# Sample Data Generation
now_ts = now()

# 5 schools with diverse characteristics
school_names = ["Lincoln High School", "Roosevelt Elementary", "Washington Middle School", "Jefferson Academy", "Madison Prep"]
school_addresses = ["123 Education Ave", "456 Learning St", "789 Knowledge Blvd", "321 Wisdom Way", "654 Scholar Dr"]
school_ids = [gen_id() for _ in range(5)]
schools_data = [
    (school_ids[i], now_ts, now_ts, True, '{}', school_names[i], school_addresses[i])
    for i in range(5)
]

users_data = []
teacher_ids = []
student_ids = []
parent_ids = []
clerk_ids = []

# Realistic names and data
teacher_names = [
    "Dr. Sarah Johnson", "Prof. Michael Chen", "Ms. Emily Rodriguez", "Mr. David Thompson",
    "Dr. Lisa Wang", "Mr. James Wilson", "Ms. Maria Garcia", "Prof. Robert Brown",
    "Dr. Jennifer Davis", "Mr. Christopher Lee", "Ms. Amanda Taylor", "Dr. Kevin Martinez",
    "Prof. Rachel Green", "Mr. Daniel White", "Ms. Jessica Anderson"
]

student_names = [
    "Alex Johnson", "Emma Smith", "Noah Williams", "Olivia Brown", "Liam Jones",
    "Ava Garcia", "William Miller", "Sophia Davis", "James Rodriguez", "Isabella Martinez",
    "Benjamin Wilson", "Mia Anderson", "Lucas Taylor", "Charlotte Thomas", "Henry Jackson",
    "Amelia White", "Alexander Harris", "Harper Martin", "Mason Thompson", "Evelyn Garcia",
    "Ethan Martinez", "Abigail Robinson", "Michael Clark", "Emily Rodriguez", "Daniel Lewis",
    "Elizabeth Lee", "Matthew Walker", "Sofia Hall", "Jackson Allen", "Avery Young"
]

parent_names = [
    "Robert & Linda Johnson", "Maria & Carlos Rodriguez", "David & Sarah Wilson",
    "Jennifer & Michael Brown", "Lisa & James Garcia", "Patricia & Robert Davis",
    "Susan & John Miller", "Nancy & William Jones", "Karen & Thomas Anderson",
    "Betty & Charles Taylor", "Helen & Joseph Thomas", "Sandra & Richard Jackson",
    "Donna & Daniel White", "Carol & Paul Harris", "Ruth & Mark Martin"
]

clerk_names = ["Alice Thompson", "Bob Wilson", "Carol Davis", "David Brown", "Eve Garcia"]

# For each school: 3 teachers, 6 students, 3 parents, 1 clerk
for s in range(5):
    # Teachers
    for t in range(3):
        tid = gen_id()
        teacher_ids.append(tid)
        users_data.append((tid, now_ts, now_ts, True, '{}', teacher_names[s*3 + t], f"teacher{s+1}_{t+1}@school.edu", "teacher", school_ids[s]))
    
    # Students
    for st in range(6):
        sid = gen_id()
        student_ids.append(sid)
        users_data.append((sid, now_ts, now_ts, True, '{}', student_names[s*6 + st], f"student{s+1}_{st+1}@student.edu", "student", school_ids[s]))
    
    # Parents
    for p in range(3):
        pid = gen_id()
        parent_ids.append(pid)
        users_data.append((pid, now_ts, now_ts, True, '{}', parent_names[s*3 + p], f"parent{s+1}_{p+1}@parent.com", "parent", school_ids[s]))
    
    # Clerk
    cid = gen_id()
    clerk_ids.append(cid)
    users_data.append((cid, now_ts, now_ts, True, '{}', clerk_names[s], f"clerk{s+1}@district.org", "clerk", school_ids[s]))

# Courses: 3 per teacher (45 total) with diverse subjects
course_subjects = ["Mathematics", "English Literature", "Science", "History", "Art", "Music", "Physical Education", "Computer Science", "Foreign Language"]
course_levels = ["101", "102", "201", "202", "301", "302", "AP", "Honors", "Remedial"]
courses_data = []
course_ids = []

for i, tid in enumerate(teacher_ids):
    school_idx = i // 3
    for c in range(3):
        cid = gen_id()
        course_ids.append(cid)
        subject = course_subjects[i % len(course_subjects)]
        level = course_levels[c % len(course_levels)]
        course_name = f"{subject} {level}"
        courses_data.append((cid, now_ts, now_ts, True, '{}', course_name, tid, school_ids[school_idx]))

# Content: 3-5 per course with different file types
file_types = ["pdf", "docx", "pptx", "mp4", "jpg", "zip"]
content_types = ["lesson", "assignment", "quiz", "video", "image", "resource"]
contents_data = []
content_ids = []

for i, course_id in enumerate(course_ids):
    teacher_id = teacher_ids[i // 3]
    for c in range(4):  # 4 content items per course
        cntid = gen_id()
        content_ids.append(cntid)
        file_type = file_types[c % len(file_types)]
        content_type = content_types[c % len(content_types)]
        file_path = f"/files/{course_id}/{content_type}_{c+1}.{file_type}"
        contents_data.append((cntid, now_ts, now_ts, True, '{}', course_id, teacher_id, file_path, file_type))

# Student enrollments: each student takes 3-5 courses
enrollments = []  # (student_id, course_id)
import random
random.seed(42)  # For reproducible results

for i, sid in enumerate(student_ids):
    school_idx = i // 6
    school_courses = [course_ids[j] for j in range(len(course_ids)) if j // 3 // 3 == school_idx]
    num_courses = random.randint(3, 5)
    selected_courses = random.sample(school_courses, min(num_courses, len(school_courses)))
    for cid in selected_courses:
        enrollments.append((sid, cid))

# Homework: each student submits for each course they are enrolled in
homework_ids = []
homeworks_data = []

for i, (sid, cid) in enumerate(enrollments):
    hwid = gen_id()
    homework_ids.append(hwid)
    file_type = random.choice(["pdf", "docx", "txt"])
    homeworks_data.append((hwid, now_ts, now_ts, True, '{}', cid, sid, f"/files/{cid}/homework_{sid}_{i}.{file_type}", file_type))

# Grades: each homework gets a grade with realistic distribution
grade_letters = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "F"]
grade_weights = [0.05, 0.15, 0.10, 0.15, 0.15, 0.10, 0.10, 0.10, 0.05, 0.03, 0.02, 0.00]  # Realistic distribution
grade_comments = [
    "Outstanding work!", "Excellent performance!", "Very good effort!", "Good job!",
    "Well done!", "Nice work!", "Satisfactory", "Keep improving!", "Needs more effort",
    "Below expectations", "Requires significant improvement", "Incomplete work"
]

grades_data = []
grade_ids = []

for i, (hwid, now_ts, now_ts2, active, meta, cid, sid, fpath, ftype) in enumerate(homeworks_data):
    gid = gen_id()
    grade_ids.append(gid)
    grade = random.choices(grade_letters, weights=grade_weights)[0]
    comment = grade_comments[grade_letters.index(grade)]
    grades_data.append((gid, now_ts, now_ts, True, '{}', cid, sid, grade, comment))

# CourseLoadAdjustment: realistic course changes
actions = ["add", "drop", "switch", "audit"]
adjustments_data = []
adjustment_ids = []

# Each student makes 1-3 adjustments
for sid in student_ids:
    num_adjustments = random.randint(1, 3)
    student_courses = [cid for s, cid in enrollments if s == sid]
    for _ in range(num_adjustments):
        aid = gen_id()
        adjustment_ids.append(aid)
        action = random.choice(actions)
        course_id = random.choice(student_courses) if student_courses else random.choice(course_ids)
        adjustments_data.append((aid, now_ts, now_ts, True, '{}', sid, course_id, action))

# Create and populate tables
def create_and_populate(schema, data, path):
    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").save(path)

create_and_populate(school_schema, schools_data, paths["schools"])
create_and_populate(user_schema, users_data, paths["users"])
create_and_populate(course_schema, courses_data, paths["courses"])
create_and_populate(content_schema, contents_data, paths["contents"])
create_and_populate(homework_schema, homeworks_data, paths["homeworks"])
create_and_populate(grade_schema, grades_data, paths["grades"])
create_and_populate(adjustment_schema, adjustments_data, paths["adjustments"])

print("Delta tables created and populated with interrelated sample data for reporting.")
