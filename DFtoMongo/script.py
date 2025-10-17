# pip install pandas pymongo python-dateutil
import json
from datetime import datetime
from dateutil.tz import tzutc
from pymongo import MongoClient, UpdateOne
import numpy as np
from pathlib import Path
import pandas as pd

# ---------- helpers ----------
def to_dt(s):
    """Parse ISO or {dueDate, dueTime} dicts into a Python datetime (UTC)."""
    if s is None:
        return None
    if isinstance(s, str):
        try:
            # handle '2025-08-30T19:27:44.005Z'
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(tzutc())
        except Exception:
            return None
    if isinstance(s, dict) and {"year","month","day"} <= set(s.get("dueDate", s).keys()):
        d = s.get("dueDate", s)
        t = s.get("dueTime", {})
        hh, mm = int(t.get("hours", 0)), int(t.get("minutes", 0))
        return datetime(int(d["year"]), int(d["month"]), int(d["day"]), hh, mm, tzinfo=tzutc())
    return None

def df_to_docs(df: pd.DataFrame):
    """NaN->None and convert DataFrame to list of dicts."""
    if df is None or df.empty:
        return []
    return df.replace({np.nan: None}).to_dict("records")

# ---------- load ----------

json_directory = "classroom_data"

json_files = sorted(Path(json_directory).glob("classroom_data_*.json"),reverse=True)

with open(json_files[0],"r",encoding="UTF-8") as f:
    raw = json.load(f)
    
courses_raw = raw["courses"]

# ---------- build DataFrames with json_normalize ----------
# Courses (flat course_info)
courses_df = pd.json_normalize(courses_raw, record_path=None, meta=None)
courses_df = pd.json_normalize(courses_df["course_info"])  # keep just course_info
courses_df.rename(columns={"id":"courseId"}, inplace=True)
courses_df["creationTime"] = courses_df["creationTime"].map(to_dt)
courses_df["updateTime"]  = courses_df["updateTime"].map(to_dt)

# Students
students_df = pd.json_normalize(
    courses_raw,
    record_path=["students"],
    meta=[["course_info","id"]],
    sep="."
)
students_df.rename(columns={"course_info.id":"courseId"}, inplace=True)

# Teachers
teachers_df = pd.json_normalize(
    courses_raw,
    record_path=["teachers"],
    meta=[["course_info","id"]],
    sep="."
)
teachers_df.rename(columns={"course_info.id":"courseId"}, inplace=True)

# Assignments
assignments_df = pd.json_normalize(
    courses_raw,
    record_path=["assignments"],
    meta=[["course_info","id"]],
    sep="."
)
assignments_df.rename(columns={"course_info.id":"courseId", "id":"assignmentId"}, inplace=True)

# unify due datetime (may be missing on some)
assignments_df["dueDateTime"] = assignments_df.apply(
    lambda r: to_dt({"dueDate": r.get("dueDate", None), "dueTime": r.get("dueTime", None)}), axis=1
)
assignments_df["creationTime"] = assignments_df["creationTime"].map(to_dt)
assignments_df["updateTime"]   = assignments_df["updateTime"].map(to_dt)

# Submissions (1 row per submission)
submissions_df = pd.json_normalize(
    courses_raw,
    record_path=["assignments","submissions"],
    meta=[
        ["course_info","id"],                 # parent course
        ["assignments","id"],                 # parent assignment
        ["assignments","title"],              # for convenience
    ],
    sep=".",
    errors="ignore"
)
submissions_df.rename(columns={
    "course_info.id":"courseId",
    "assignments.id":"assignmentId",
    "assignments.title":"assignmentTitle",
    "id":"submissionId"
}, inplace=True)

# Parse timestamps
for col in ("creationTime","updateTime"):
    if col in submissions_df:
        submissions_df[col] = submissions_df[col].map(to_dt)

# Extract a clean attachments list (drive files only)
def extract_attachments(row):
    x = row.get("assignmentSubmission.attachments", None)
    if not isinstance(x, list):
        return None
    out = []
    for att in x:
        df = att.get("driveFile") or {}
        # two possible shapes: {"id": "...", "title": "...", "alternateLink": "..."} OR nested under "driveFile"
        inner = df.get("driveFile") if isinstance(df.get("driveFile"), dict) else df
        if isinstance(inner, dict):
            out.append({
                "id": inner.get("id"),
                "title": inner.get("title"),
                "link": inner.get("alternateLink"),
                "thumb": inner.get("thumbnailUrl")
            })
    return out or None

submissions_df["attachments"] = submissions_df.apply(extract_attachments, axis=1)

# Latest grade (if present in gradeHistory)
def pick_latest_points(hist):
    if not isinstance(hist, list) or not hist:
        return None
    # keep last entry with pointsEarned
    pts = None
    for h in hist:
        gh = h.get("gradeHistory") if isinstance(h, dict) else None
        if isinstance(gh, dict) and "pointsEarned" in gh:
            pts = gh["pointsEarned"]
    return pts

submissions_df["pointsEarned_latest"] = submissions_df["submissionHistory"].map(pick_latest_points)
submissions_df["late"] = submissions_df.get("late", False).fillna(False)

# Keep only practical columns (optional; drop the massive histories if you want)
keep_sub_cols = [
    "submissionId","courseId","assignmentId","assignmentTitle","userId",
    "state","creationTime","updateTime","late","draftGrade",
    "pointsEarned_latest","attachments","alternateLink"
]
submissions_df = submissions_df[[c for c in keep_sub_cols if c in submissions_df.columns]]

# ---------- write to MongoDB (upserts with indexes) ----------
client = MongoClient("mongodb://localhost:27017")  # change to your URI
db = client["classroom"]
cols = {
    "courses": db.courses,
    "students": db.students,
    "teachers": db.teachers,
    "assignments": db.assignments,
    "submissions": db.submissions,
}

# Create unique indexes (safe to rerun)
cols["courses"].create_index("courseId", unique=True)
cols["students"].create_index([("courseId", 1), ("userId", 1)], unique=True)
cols["teachers"].create_index([("courseId", 1), ("userId", 1)], unique=True)
cols["assignments"].create_index([("courseId", 1), ("assignmentId", 1)], unique=True)
cols["submissions"].create_index([("assignmentId", 1), ("submissionId", 1)], unique=True)

def upsert(df, col, key_fields):
    docs = df_to_docs(df)
    if not docs:
        return
    ops = []
    for d in docs:
        filt = {k: d[k] for k in key_fields}
        ops.append(UpdateOne(filt, {"$set": d}, upsert=True))
    col.bulk_write(ops, ordered=False)

upsert(courses_df,    cols["courses"],    ["courseId"])
upsert(students_df,   cols["students"],   ["courseId","userId"])
upsert(teachers_df,   cols["teachers"],   ["courseId","userId"])
upsert(assignments_df,cols["assignments"],["courseId","assignmentId"])
upsert(submissions_df,cols["submissions"],["assignmentId","submissionId"])

print("Done.")
