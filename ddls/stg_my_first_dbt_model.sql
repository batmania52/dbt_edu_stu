CREATE TABLE stg.my_first_dbt_model (
    id integer
);

COMMENT ON TABLE stg.my_first_dbt_model IS '처음으로 생성된 dbt 모델';
COMMENT ON COLUMN stg.my_first_dbt_model.id IS '고유 식별자';
