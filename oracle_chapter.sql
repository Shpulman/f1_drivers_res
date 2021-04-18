--Создание таблиц
CREATE TABLE f1_drivers (
d_driverId      NUMBER(10) PRIMARY KEY,
d_driverRef     VARCHAR2(100) ,
d_number        NUMBER(10), --NUMBER
d_code          VARCHAR2(10) ,
d_forename      VARCHAR2(100) ,
d_surname       VARCHAR2(100) ,
d_dob           DATE, --DATE
d_nationality   VARCHAR2(100),
d_url           VARCHAR2(100)
)
/

/
CREATE TABLE f1_races (
r_raceId    NUMBER(10) PRIMARY KEY,
r_year      NUMBER(10),
r_round     NUMBER(10) ,
r_circuitId NUMBER(10),
r_name      VARCHAR2(100),
r_date      DATE,
r_time      VARCHAR2(100),
r_url       VARCHAR2(100)
)
/
CREATE TABLE f1_results (
re_resultId         NUMBER(10) PRIMARY KEY,
re_raceId           NUMBER(10) ,
re_driverId         NUMBER(10) ,
re_constructorId    NUMBER(10),
re_number           NUMBER(10),
re_grid             NUMBER(10),
re_position         NUMBER(10),
re_positionText     VARCHAR2(20),
re_positionOrder    NUMBER(10),
re_points           NUMBER(10),
re_laps             NUMBER(10),
re_time             VARCHAR2(100),
re_milliseconds     NUMBER(20),
re_fastestLap       NUMBER(10),
re_rank             NUMBER(10),
re_fastestLapTime   VARCHAR2(100),
re_fastestLapSpeed  NUMBER(10),
re_statusId         NUMBER(10),
CONSTRAINT re_raceId_fk FOREIGN KEY (re_raceId) REFERENCES f1_races (r_raceId),
CONSTRAINT re_driverId_fk FOREIGN KEY (re_driverId) REFERENCES f1_drivers (d_driverId)
)
/
SELECT *
FROM f1_drivers
ORDER BY 1
/
SELECT  *
FROM f1_races
/
SELECT *
FROM f1_results
ORDER BY 1
/
SELECT  re.re_driverid as "ID гонщика",
        d.d_forename AS Имя,
        d.d_surname AS Фамилия,
        d.d_dob AS "Дата рождения",
        d.d_number AS "Гоночный номер",
        (
            SELECT  SUM (re1.re_grid)
            FROM    f1_results re1
            WHERE   1=1
                    AND re1.re_grid = 1
                    AND re1.re_driverid = re.re_driverid
            GROUP BY re1.re_driverid 
        ) AS "Поул позишн",
        (
            SELECT  SUM (re2.re_positionorder)
            FROM    f1_results re2
            WHERE   1=1
                    AND re2.re_positionorder = 1
                    AND re2.re_driverid = re.re_driverid
            GROUP BY re2.re_driverid 
        ) AS "Победы",
        SUM(re.re_points) AS "Сумм набр. очков",
        MIN (r.r_year) AS "Первая гонка",
        MAX (r.r_year) AS "Последняя гонка"       
FROM    f1_drivers d 
        JOIN f1_results re ON re.re_driverid = d.d_driverid
        JOIN f1_races r ON re.re_raceid = r.r_raceid
WHERE   1=1
GROUP BY re.re_driverid, d.d_forename,d.d_surname, d.d_number, d.d_dob
ORDER BY 1 DESC
/
DROP TABLE f1_drivers_results
/
CREATE TABLE f1_drivers_results (
driver_id       NUMBER(10) PRIMARY KEY,
first_name      VARCHAR2(100) ,
last_name       VARCHAR2(100), --NUMBER
bd              DATE ,
race_namber     NUMBER(10) ,
pole_position   NUMBER(10) ,
wins            NUMBER(10), --DATE
points          NUMBER(10),
first_race      NUMBER(10),
last_race       NUMBER(10)
)
/
SET SERVEROUTPUT ON 
/
DROP TABLE f1_drivers_results
/
DROP PROCEDURE f1_drivers_results_proc
/
CREATE OR REPLACE PROCEDURE f1_drivers_results_proc
    IS
    CURSOR  cur_f1 IS
        SELECT  re.re_driverid as "ID гонщика",
        d.d_forename AS Имя,
        d.d_surname AS Фамилия,
        d.d_dob AS "Дата рождения",
        d.d_number AS "Гоночный номер",
        (
            SELECT  SUM (re1.re_grid)
            FROM    f1_results re1
            WHERE   1=1
                    AND re1.re_grid = 1
                    AND re1.re_driverid = re.re_driverid
            GROUP BY re1.re_driverid 
        ) AS "Поул позишн",
        (
            SELECT  SUM (re2.re_positionorder)
            FROM    f1_results re2
            WHERE   1=1
                    AND re2.re_positionorder = 1
                    AND re2.re_driverid = re.re_driverid
            GROUP BY re2.re_driverid 
        ) AS "Победы",
        SUM(re.re_points) AS "Сумм набр. очков",
        MIN (r.r_year) AS "Первая гонка",
        MAX (r.r_year) AS "Последняя гонка"       
FROM    f1_drivers d 
        JOIN f1_results re ON re.re_driverid = d.d_driverid
        JOIN f1_races r ON re.re_raceid = r.r_raceid
WHERE   1=1
GROUP BY re.re_driverid, d.d_forename,d.d_surname, d.d_number, d.d_dob
ORDER BY 1 DESC;
        v_driver_id         f1_drivers_results.driver_id%TYPE;
        v_first_name        f1_drivers_results.first_name%TYPE;
        v_last_name         f1_drivers_results.last_name%TYPE;
        v_bd                f1_drivers_results.bd%TYPE;
        v_race_namber       f1_drivers_results.race_namber%TYPE;
        v_pole_position     f1_drivers_results.pole_position%TYPE;
        v_wins              f1_drivers_results.wins%TYPE;
        v_points            f1_drivers_results.points%TYPE;
        v_first_race        f1_drivers_results.first_race%TYPE;
        v_last_race         f1_drivers_results.last_race%TYPE;
        v_count_row         INTEGER;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE f1_drivers_results';
    OPEN cur_f1;
    LOOP
        FETCH cur_f1 INTO v_driver_id, v_first_name, v_last_name, v_bd, v_race_namber, v_pole_position, v_wins, v_points, v_first_race, v_last_race;
        DBMS_OUTPUT.PUT_LINE (v_first_name);
        EXIT WHEN cur_f1%NOTFOUND;
        INSERT INTO f1_drivers_results
            VALUES (v_driver_id, v_first_name, v_last_name, v_bd, v_race_namber, v_pole_position, v_wins, v_points, v_first_race, v_last_race);
              IF MOD(cur_f1%ROWCOUNT, 5) = 0 THEN
            COMMIT;
        END IF;
    END LOOP;
    --CLOSE cur_f1;
    COMMIT;
        DBMS_OUTPUT.PUT_LINE('Вставлено строк в таблицу = '|| cur_f1%ROWCOUNT);
    SELECT COUNT(sr.driver_id)
    INTO v_count_row
    FROM f1_drivers_results sr;
    IF cur_f1%ROWCOUNT = v_count_row THEN
        DBMS_OUTPUT.PUT_LINE('Совпадает с количеством строк в таблице с гонщиками');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Не совпадает с количеством строк в таблице с гонщиками');
    END IF;
    COMMIT;
END;
/

create or replace PACKAGE pkg_f1_drivers_res
AS 
    PROCEDURE f1_drivers_results_proc;
END pkg_f1_drivers_res;
/
create or replace PACKAGE BODY pkg_f1_drivers_res
AS
PROCEDURE f1_drivers_results_proc
    IS
    CURSOR  cur_f1 IS
        SELECT  re.re_driverid as "ID гонщика",
        d.d_forename AS Имя,
        d.d_surname AS Фамилия,
        d.d_dob AS "Дата рождения",
        d.d_number AS "Гоночный номер",
        (
            SELECT  SUM (re1.re_grid)
            FROM    f1_results re1
            WHERE   1=1
                    AND re1.re_grid = 1
                    AND re1.re_driverid = re.re_driverid
            GROUP BY re1.re_driverid 
        ) AS "Поул позишн",
        (
            SELECT  SUM (re2.re_positionorder)
            FROM    f1_results re2
            WHERE   1=1
                    AND re2.re_positionorder = 1
                    AND re2.re_driverid = re.re_driverid
            GROUP BY re2.re_driverid 
        ) AS "Победы",
        SUM(re.re_points) AS "Сумм набр. очков",
        MIN (r.r_year) AS "Первая гонка",
        MAX (r.r_year) AS "Последняя гонка"       
FROM    f1_drivers d 
        JOIN f1_results re ON re.re_driverid = d.d_driverid
        JOIN f1_races r ON re.re_raceid = r.r_raceid
WHERE   1=1
GROUP BY re.re_driverid, d.d_forename,d.d_surname, d.d_number, d.d_dob
ORDER BY 1 DESC;
        v_driver_id         f1_drivers_results.driver_id%TYPE;
        v_first_name        f1_drivers_results.first_name%TYPE;
        v_last_name         f1_drivers_results.last_name%TYPE;
        v_bd                f1_drivers_results.bd%TYPE;
        v_race_namber       f1_drivers_results.race_namber%TYPE;
        v_pole_position     f1_drivers_results.pole_position%TYPE;
        v_wins              f1_drivers_results.wins%TYPE;
        v_points            f1_drivers_results.points%TYPE;
        v_first_race        f1_drivers_results.first_race%TYPE;
        v_last_race         f1_drivers_results.last_race%TYPE;
        v_count_row         INTEGER;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE f1_drivers_results';
    OPEN cur_f1;
    LOOP
        FETCH cur_f1 INTO v_driver_id, v_first_name, v_last_name, v_bd, v_race_namber, v_pole_position, v_wins, v_points, v_first_race, v_last_race;
        DBMS_OUTPUT.PUT_LINE (v_first_name);
        EXIT WHEN cur_f1%NOTFOUND;
        INSERT INTO f1_drivers_results
            VALUES (v_driver_id, v_first_name, v_last_name, v_bd, v_race_namber, v_pole_position, v_wins, v_points, v_first_race, v_last_race);
              IF MOD(cur_f1%ROWCOUNT, 5) = 0 THEN
            COMMIT;
        END IF;
    END LOOP;
    --CLOSE cur_f1;
    COMMIT;
        DBMS_OUTPUT.PUT_LINE('Вставлено строк в таблицу = '|| cur_f1%ROWCOUNT);
    SELECT COUNT(sr.driver_id)
    INTO v_count_row
    FROM f1_drivers_results sr;
    IF cur_f1%ROWCOUNT = v_count_row THEN
        DBMS_OUTPUT.PUT_LINE('Совпадает с количеством строк в таблице с гонщиками');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Не совпадает с количеством строк в таблице с гонщиками');
    END IF;
    COMMIT;
END;
END pkg_f1_drivers_res;
/
BEGIN
    pkg_f1_drivers_res.f1_drivers_results_proc;
END;
/
SELECT *
FROM f1_drivers_results
ORDER BY 1 DESC

/

DELETE FROM f1_drivers_results

/


 
 


 
        













