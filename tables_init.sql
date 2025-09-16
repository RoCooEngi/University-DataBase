-- inits all the tables of database
CREATE TABLE IF NOT EXISTS institutes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS departments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL,
    institute_id INTEGER NOT NULL,
    FOREIGN KEY (institute_id) REFERENCES institutes(id)
);

CREATE TABLE IF NOT EXISTS programs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    department_id INTEGER NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

CREATE TABLE IF NOT EXISTS subjects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    semester INTEGER NOT NULL,
    eval_method TEXT,
    url TEXT,
    program_id INTEGER NOT NULL,
    FOREIGN KEY (program_id) REFERENCES programs(id)
);

-- CREATE TABLE IF NOT EXISTS students (
--     id INTEGER PRIMARY KEY,
--     name TEXT NOT NULL,
--     study_year INTEGER NOT NULL,

-- );