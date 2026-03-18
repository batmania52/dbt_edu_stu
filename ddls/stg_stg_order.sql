CREATE TABLE stg.stg_order (
    order_id integer,
    customer_id integer,
    order_date timestamp without time zone,
    total_amount numeric(10, 2)
);

COMMENT ON TABLE stg.stg_order IS '주문 데이터의 스테이징 모델';
COMMENT ON COLUMN stg.stg_order.order_id IS '주문의 기본 키';
COMMENT ON COLUMN stg.stg_order.customer_id IS '고객에 대한 외래 키';
COMMENT ON COLUMN stg.stg_order.order_date IS '주문 날짜';
COMMENT ON COLUMN stg.stg_order.total_amount IS '주문 총액';
