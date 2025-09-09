from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("SchoolDistrictDW-Queries") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

base_path = "/tmp/delta"

users = spark.read.format("delta").load(f"{base_path}/users")
schools = spark.read.format("delta").load(f"{base_path}/schools")
courses = spark.read.format("delta").load(f"{base_path}/courses")
contents = spark.read.format("delta").load(f"{base_path}/contents")
homeworks = spark.read.format("delta").load(f"{base_path}/homeworks")
grades = spark.read.format("delta").load(f"{base_path}/grades")
adjustments = spark.read.format("delta").load(f"{base_path}/adjustments")

users.createOrReplaceTempView("users")
schools.createOrReplaceTempView("schools")
courses.createOrReplaceTempView("courses")
contents.createOrReplaceTempView("contents")
homeworks.createOrReplaceTempView("homeworks")
grades.createOrReplaceTempView("grades")
adjustments.createOrReplaceTempView("adjustments")

# 1. Average grade per course
avg_grade_per_course = spark.sql('''
SELECT c.name AS course_name, AVG(
  CASE grade
    WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7 WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7 WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 ELSE 0 END
) AS avg_gpa
FROM grades g
JOIN courses c ON g.course_id = c.id
GROUP BY c.name
''')

# 2. Average grade per teacher
avg_grade_per_teacher = spark.sql('''
SELECT u.name AS teacher_name, AVG(
  CASE grade
    WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7 WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7 WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 ELSE 0 END
) AS avg_gpa
FROM grades g
JOIN courses c ON g.course_id = c.id
JOIN users u ON c.teacher_id = u.id
WHERE u.role = 'teacher'
GROUP BY u.name
''')

# 3. Homework submission rate per course
homework_submission_rate = spark.sql('''
SELECT c.name AS course_name, COUNT(h.id) AS num_homeworks
FROM courses c
LEFT JOIN homeworks h ON h.course_id = c.id
GROUP BY c.name
''')

# 4. Parent engagement: number of children and their average grade
# (Assume parent is linked to students by school, and each parent has 2 students in the same school)
parent_engagement = spark.sql('''
SELECT p.name AS parent_name, COUNT(s.id) AS num_children, AVG(
  CASE g.grade
    WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7 WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7 WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 ELSE 0 END
) AS avg_gpa
FROM users p
JOIN users s ON p.school_id = s.school_id AND s.role = 'student'
LEFT JOIN grades g ON g.student_id = s.id
WHERE p.role = 'parent'
GROUP BY p.name
''')

# 5. Course load changes by student
course_load_changes = spark.sql('''
SELECT u.name AS student_name, COUNT(a.id) AS num_changes
FROM adjustments a
JOIN users u ON a.student_id = u.id
WHERE u.role = 'student'
GROUP BY u.name
''')

# 6. Grade distribution analysis
grade_distribution = spark.sql('''
SELECT grade, COUNT(*) as count, 
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM grades
GROUP BY grade
ORDER BY 
  CASE grade
    WHEN 'A+' THEN 1 WHEN 'A' THEN 2 WHEN 'A-' THEN 3
    WHEN 'B+' THEN 4 WHEN 'B' THEN 5 WHEN 'B-' THEN 6
    WHEN 'C+' THEN 7 WHEN 'C' THEN 8 WHEN 'C-' THEN 9
    WHEN 'D+' THEN 10 WHEN 'D' THEN 11 WHEN 'F' THEN 12
  END
''')

# 7. School performance comparison
school_performance = spark.sql('''
SELECT s.name as school_name, 
       COUNT(DISTINCT g.student_id) as total_students,
       AVG(CASE grade
         WHEN 'A+' THEN 4.0 WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7
         WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7
         WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 WHEN 'C-' THEN 1.7
         WHEN 'D+' THEN 1.3 WHEN 'D' THEN 1.0 WHEN 'F' THEN 0.0
       END) as avg_gpa,
       COUNT(g.id) as total_grades
FROM schools s
JOIN users u ON s.id = u.school_id
JOIN grades g ON u.id = g.student_id
WHERE u.role = 'student'
GROUP BY s.name
''')

# 8. Subject performance analysis
subject_performance = spark.sql('''
SELECT 
  CASE 
    WHEN c.name LIKE '%Mathematics%' THEN 'Mathematics'
    WHEN c.name LIKE '%English%' THEN 'English Literature'
    WHEN c.name LIKE '%Science%' THEN 'Science'
    WHEN c.name LIKE '%History%' THEN 'History'
    WHEN c.name LIKE '%Art%' THEN 'Art'
    WHEN c.name LIKE '%Music%' THEN 'Music'
    WHEN c.name LIKE '%Computer%' THEN 'Computer Science'
    ELSE 'Other'
  END as subject,
  AVG(CASE grade
    WHEN 'A+' THEN 4.0 WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7
    WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7
    WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 WHEN 'C-' THEN 1.7
    WHEN 'D+' THEN 1.3 WHEN 'D' THEN 1.0 WHEN 'F' THEN 0.0
  END) as avg_gpa,
  COUNT(DISTINCT g.student_id) as unique_students,
  COUNT(g.id) as total_assignments
FROM courses c
JOIN grades g ON c.id = g.course_id
GROUP BY 
  CASE 
    WHEN c.name LIKE '%Mathematics%' THEN 'Mathematics'
    WHEN c.name LIKE '%English%' THEN 'English Literature'
    WHEN c.name LIKE '%Science%' THEN 'Science'
    WHEN c.name LIKE '%History%' THEN 'History'
    WHEN c.name LIKE '%Art%' THEN 'Art'
    WHEN c.name LIKE '%Music%' THEN 'Music'
    WHEN c.name LIKE '%Computer%' THEN 'Computer Science'
    ELSE 'Other'
  END
''')

# 9. Teacher workload analysis
teacher_workload = spark.sql('''
SELECT u.name as teacher_name, s.name as school_name,
       COUNT(DISTINCT c.id) as courses_taught,
       COUNT(DISTINCT g.student_id) as students_taught,
       COUNT(g.id) as total_grades_given,
       AVG(CASE grade
         WHEN 'A+' THEN 4.0 WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7
         WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7
         WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 WHEN 'C-' THEN 1.7
         WHEN 'D+' THEN 1.3 WHEN 'D' THEN 1.0 WHEN 'F' THEN 0.0
       END) as avg_gpa_given
FROM users u
JOIN schools s ON u.school_id = s.id
JOIN courses c ON u.id = c.teacher_id
LEFT JOIN grades g ON c.id = g.course_id
WHERE u.role = 'teacher'
GROUP BY u.name, s.name
''')

# 10. Content type analysis
content_analysis = spark.sql('''
SELECT file_type, COUNT(*) as count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM contents
GROUP BY file_type
ORDER BY count DESC
''')

# Save results as Delta tables for dashboard use
avg_grade_per_course.write.format("delta").mode("overwrite").save(f'{base_path}/analytics/avg_grade_per_course')
avg_grade_per_teacher.write.format("delta").mode("overwrite").save(f'{base_path}/analytics/avg_grade_per_teacher')
homework_submission_rate.write.format("delta").mode("overwrite").save(f'{base_path}/analytics/homework_submission_rate')
parent_engagement.write.format("delta").mode("overwrite").save(f'{base_path}/analytics/parent_engagement')
course_load_changes.write.format("delta").mode("overwrite").save(f'{base_path}/analytics/course_load_changes')
grade_distribution.write.format("delta").mode("overwrite").save(f'{base_path}/analytics/grade_distribution')
school_performance.write.format("delta").mode("overwrite").save(f'{base_path}/analytics/school_performance')
subject_performance.write.format("delta").mode("overwrite").save(f'{base_path}/analytics/subject_performance')
teacher_workload.write.format("delta").mode("overwrite").save(f'{base_path}/analytics/teacher_workload')
content_analysis.write.format("delta").mode("overwrite").save(f'{base_path}/analytics/content_analysis')

print("Analytics queries executed and results saved as Delta tables.")
