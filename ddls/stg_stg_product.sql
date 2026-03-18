CREATE TABLE stg.stg_product (
    product_id integer,
    product_name character varying,
    product_category character varying,
    price numeric(10, 2),
    created_date date
);

COMMENT ON TABLE stg.stg_product IS '상품 데이터의 스테이징 모델';
COMMENT ON COLUMN stg.stg_product.product_id IS '상품의 기본 키';
COMMENT ON COLUMN stg.stg_product.product_name IS '상품 이름';
COMMENT ON COLUMN stg.stg_product.product_category IS '상품 카테고리';
COMMENT ON COLUMN stg.stg_product.price IS '상품 가격';
COMMENT ON COLUMN stg.stg_product.created_date IS '상품 생성일';
