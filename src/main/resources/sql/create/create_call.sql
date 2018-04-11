CREATE TABLE IF NOT EXISTS Call (
    id INT PRIMARY KEY,
    subs_from LONG,
    subs_to LONG,
    dur INT,
    start_time VARCHAR)