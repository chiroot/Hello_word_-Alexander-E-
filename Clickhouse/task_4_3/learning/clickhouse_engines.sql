-- MergeTree
CREATE TABLE users_mt (
    id UInt32
    , name String
    , age UInt8
) ENGINE = MergeTree()
ORDER BY
id;

-- Вставим данные:
INSERT
    INTO
    users_mt
VALUES (1, 'Alice', 30)
       , (2, 'Bob', 25)
       , (3, 'Alice', 22);

-- Cмотрим данные в таблице users_mt c id = 1:
SELECT
    *
FROM
    users_mt
WHERE
    id = 1;

-- ReplacingMergeTree — замена строк по ключу
CREATE TABLE users_replace (
    id UInt32
    , name String
    , version UInt32
) ENGINE = ReplacingMergeTree(version)
ORDER BY
id;

-- Вставим две версии одной записи:
INSERT
    INTO
    users_replace
VALUES (1, 'Alice', 1)
       , (1, 'Alice Smith', 2);

-- Позже после слияния останется только строка с версией 2
SELECT
    *
FROM
    users_replace;



