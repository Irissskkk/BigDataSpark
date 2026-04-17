DO $$
DECLARE
    i INTEGER;
    file_path TEXT;
BEGIN
    FOR i IN 0..9 LOOP
        file_path := '/data/mock_data_' || i || '.csv';
        BEGIN
            EXECUTE format('
                COPY public.mock_data 
                FROM %L 
                DELIMITER '','' 
                CSV HEADER 
                QUOTE ''"''
            ', file_path);
            RAISE NOTICE 'Импортирован файл: %', file_path;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Ошибка импорта файла %: %', file_path, SQLERRM;
        END;
    END LOOP;
END $$;