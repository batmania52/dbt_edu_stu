CREATE TABLE stg.stg_customer (
    customer_id integer,
    customer_name character varying,
    customer_email character varying,
    registration_date date
);

COMMENT ON TABLE stg.stg_customer IS '고객 데이터의 스테이징 모델';
COMMENT ON COLUMN stg.stg_customer.customer_id IS '고객의 기본 키';
COMMENT ON COLUMN stg.stg_customer.customer_name IS '고객 이름';
COMMENT ON COLUMN stg.stg_customer.customer_email IS '고객 이메일';
COMMENT ON COLUMN stg.stg_customer.registration_date IS '고객 등록일';
