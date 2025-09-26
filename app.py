from flask import Flask, render_template, request, redirect, url_for
import sqlite3
import os

# Инициализация Flask
app = Flask(__name__,
            template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
            static_folder=os.path.join(os.path.dirname(__file__), 'static'))

# Подключение к базе данных
def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), 'university.db')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found at {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# Главная страница (институты)
@app.route('/')
def index():
    try:
        conn = get_db_connection()
        institutes = conn.execute('SELECT * FROM institutes').fetchall()
        conn.close()
        return render_template('institutes.html', institutes=institutes)
    except Exception as e:
        return f"Error: {str(e)}", 500

# Страница кафедр
@app.route('/institute/<int:institute_id>/departments')
def departments(institute_id):
    try:
        conn = get_db_connection()
        departments = conn.execute('SELECT * FROM departments WHERE institute_id = ?', (institute_id,)).fetchall()
        conn.close()
        return render_template('departments.html', departments=departments, institute_id=institute_id)
    except Exception as e:
        return f"Error: {str(e)}", 500

# Страница направлений
@app.route('/department/<int:department_id>/programs')
def programs(department_id):
    try:
        conn = get_db_connection()
        programs = conn.execute('SELECT * FROM programs WHERE department_id = ?', (department_id,)).fetchall()
        groups = conn.execute('SELECT id, name, program_id FROM groups WHERE program_id IN (SELECT id FROM programs WHERE department_id = ?)', (department_id,)).fetchall()
        conn.close()
        return render_template('programs.html', programs=programs, groups=groups, department_id=department_id)
    except Exception as e:
        return f"Error: {str(e)}", 500

# Страница учебного плана
@app.route('/program/<int:program_id>/curriculum')
def curriculum(program_id):
    try:
        conn = get_db_connection()
        subjects = conn.execute('SELECT * FROM subjects WHERE program_id = ?', (program_id,)).fetchall()
        conn.close()
        return render_template('curriculum.html', subjects=subjects, program_id=program_id)
    except Exception as e:
        return f"Error: {str(e)}", 500

# Страница группы
@app.route('/program/<int:program_id>/group/<int:group_id>')
def group(program_id, group_id):
    try:
        conn = get_db_connection()
        group = conn.execute('SELECT * FROM groups WHERE id = ? AND program_id = ?', (group_id, program_id)).fetchone()
        students = conn.execute('SELECT * FROM students WHERE group_id = ?', (group_id,)).fetchall()
        current_semester = conn.execute('SELECT course_year FROM groups WHERE id = ?', (group_id,)).fetchone()['course_year']
        schedules = {}
        for student in students:
            schedule = conn.execute('''
                SELECT subjects.name, grades.grade
                FROM subjects
                JOIN grades ON subjects.id = grades.subject_id
                WHERE grades.student_id = ? AND subjects.semester = ?
            ''', (student['id'], current_semester)).fetchall()
            schedules[student['id']] = schedule
        conn.close()
        return render_template('group.html', group=group, students=students, schedules=schedules, program_id=program_id)
    except Exception as e:
        return f"Error: {str(e)}", 500

# Добавление студента
@app.route('/group/<int:group_id>/add_student', methods=['POST'])
def add_student(group_id):
    try:
        conn = get_db_connection()
        name = request.form['name']
        scholarship = 1 if request.form.get('scholarship') else 0
        program_id = request.form['program_id']
        if not name:
            conn.close()
            return "Error: Name is required", 400
        conn.execute('INSERT INTO students (name, group_id, scholarship) VALUES (?, ?, ?)', (name, group_id, scholarship))
        conn.commit()
        conn.close()
        return redirect(url_for('group', program_id=program_id, group_id=group_id))
    except Exception as e:
        return f"Error: {str(e)}", 500

# Редактирование студента и зачетки (оценок)
@app.route('/group/<int:group_id>/edit_student/<int:student_id>', methods=['GET', 'POST'])
def edit_student(group_id, student_id):
    try:
        conn = get_db_connection()
        if group_id is None or student_id is None:
            conn.close()
            return "Error: Invalid group or student ID", 400
        
        student = conn.execute('SELECT * FROM students WHERE id = ?', (student_id,)).fetchone()
        if not student or student['group_id'] != group_id:
            conn.close()
            return "Student not found or not in this group", 404
        
        if request.method == 'POST':
            name = request.form.get('name', '').strip()
            if not name:
                error = "Name is required"
                grades = conn.execute('''
                    SELECT subjects.id AS subject_id, subjects.name, grades.grade
                    FROM subjects
                    LEFT JOIN grades ON grades.subject_id = subjects.id AND grades.student_id = ?
                    WHERE subjects.program_id = (SELECT program_id FROM groups WHERE id = ?) AND subjects.semester <= (SELECT course_year FROM groups WHERE id = ?)
                ''', (student_id, group_id, group_id)).fetchall()
                program_id = conn.execute('SELECT program_id FROM groups WHERE id = ?', (group_id,)).fetchone()['program_id']
                conn.close()
                return render_template('edit_student.html', student=student, group_id=group_id, program_id=program_id, grades=grades, error=error)
            scholarship = 1 if request.form.get('scholarship') else 0
            program_id = request.form.get('program_id')
            if not program_id:
                group = conn.execute('SELECT program_id FROM groups WHERE id = ?', (group_id,)).fetchone()
                program_id = group['program_id'] if group else None
            if not program_id:
                conn.close()
                return "Error: Program ID not found", 500
            
            # Обновление данных студента
            conn.execute('UPDATE students SET name = ?, scholarship = ? WHERE id = ?', (name, scholarship, student_id))
            
            # Обновление оценок
            for key, value in request.form.items():
                if key.startswith('grade_') and value.strip():
                    subject_id = int(key.replace('grade_', ''))
                    try:
                        grade = float(value) if value != '' else None
                        conn.execute('''
                            INSERT OR REPLACE INTO grades (student_id, subject_id, grade)
                            VALUES (?, ?, ?)
                        ''', (student_id, subject_id, grade))
                    except ValueError:
                        continue  # Пропускаем некорректные значения
            
            conn.commit()
            conn.close()
            return redirect(url_for('group', program_id=program_id, group_id=group_id))
        
        # Для GET-запроса загружаем данные
        group = conn.execute('SELECT program_id, course_year FROM groups WHERE id = ?', (group_id,)).fetchone()
        program_id = group['program_id'] if group else None
        current_semester = group['course_year'] if group else None
        grades = []
        if program_id and current_semester:
            grades = conn.execute('''
                SELECT subjects.id AS subject_id, subjects.name, grades.grade
                FROM subjects
                LEFT JOIN grades ON grades.subject_id = subjects.id AND grades.student_id = ?
                WHERE subjects.program_id = ? AND subjects.semester <= ?
            ''', (student_id, program_id, current_semester)).fetchall()
        conn.close()
        return render_template('edit_student.html', student=student, group_id=group_id, program_id=program_id, grades=grades or [])
    except ValueError as e:
        conn.close()
        return f"Error: Invalid input - {str(e)}", 400
    except Exception as e:
        return f"Error: {str(e)}", 500

# Удаление студента
@app.route('/group/<int:group_id>/delete_student/<int:student_id>', methods=['POST'])
def delete_student(group_id, student_id):
    try:
        conn = get_db_connection()
        student = conn.execute('SELECT * FROM students WHERE id = ?', (student_id,)).fetchone()
        if not student or student['group_id'] != group_id:
            conn.close()
            return "Student not found or not in this group", 404
        
        program_id = request.form.get('program_id')
        if not program_id:
            group = conn.execute('SELECT program_id FROM groups WHERE id = ?', (group_id,)).fetchone()
            program_id = group['program_id'] if group else None
        conn.execute('DELETE FROM students WHERE id = ?', (student_id,))
        conn.commit()
        conn.close()
        return redirect(url_for('group', program_id=program_id, group_id=group_id)) if program_id else ("Error: Program ID not found", 500)
    except Exception as e:
        return f"Error: {str(e)}", 500

if __name__ == '__main__':
    app.run(debug=True)