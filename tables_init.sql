-- inits all the tables of database
CREATE TABLE IF NOT EXISTS departments (
    department_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS programs (
    program_id INTEGER PRIMARY KEY AUTOINCREMENT,
    department_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

CREATE TABLE IF NOT EXISTS subjects (
    subject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS program_subjects (
    program_subject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    program_id INTEGER NOT NULL,
    subject_id INTEGER NOT NULL,
    semester INTEGER NOT NULL,
    FOREIGN KEY (program_id) REFERENCES programs(program_id),
    FOREIGN KEY (subject_id) REFERENCES subjects(subject_id)
);

CREATE TABLE IF NOT EXISTS student_groups (
    group_id INTEGER PRIMARY KEY AUTOINCREMENT,
    program_id INTEGER NOT NULL,
    course INTEGER NOT NULL,
    name TEXT NOT NULL,
    FOREIGN KEY (program_id) REFERENCES programs(program_id)
);

CREATE TABLE IF NOT EXISTS students (
    student_id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL,
    full_name TEXT NOT NULL,
    record_book TEXT UNIQUE NOT NULL,
    scholarship REAL,
    FOREIGN KEY (group_id) REFERENCES student_groups(group_id)
);