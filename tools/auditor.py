"""
dbt Data Integrity Auditor (Accurate Loop Version)
==================================================

사용자의 dbt 모델 내 매크로(get_date_intervals)가 동적으로 동작해야 하므로,
매 루프마다 dbt compile을 수행하여 정확한 날짜 구간의 SQL을 획득합니다.

주의사항:
----------
- dbt 모델 내에서 `run_query`를 사용하는 경우, 반드시 `{% if execute %}` 블록으로 감싸야 합니다.
  (dbt 내부 변수명은 `is_execute`가 아닌 `execute`입니다.)
  그렇지 않으면 `auditor.py`가 수행하는 컴파일 단계에서 오류가 발생할 수 있습니다.
- `--dry-run` 옵션을 사용하면 실제 DB 조회를 수행하지 않고 쿼리 구조만 확인할 수 있습니다.
"""

import subprocess
import os
import json
import sys
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta

# ==========================================
# 1. 환경 설정 및 경로
# ==========================================
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CONF_PATH = os.path.join(project_root, 'dbconf.json')
DBT_PROJECT_DIR = os.path.join(project_root, 'edu')

# ==========================================
# 2. DB 유틸리티 함수
# ==========================================
def get_db_engine():
    try:
        with open(CONF_PATH, 'r') as f:
            config = json.load(f)
        db = config["postgres_remote"]
        url = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
        return create_engine(url)
    except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        print(f"🚨 [CONFIG ERROR] 설정 파일을 읽는 중 오류 발생: {e}")
        sys.exit(1)

def execute_query(query, engine, label="Query", is_execute=True):
    """
    쿼리를 실행하고 결과를 반환합니다.
    is_execute가 False일 경우 실제 쿼리를 실행하지 않는 Dry Run 모드로 동작합니다.
    """
    if not is_execute:
        print(f"[*] [Dry Run] {label} 스킵 (쿼리 생성 완료)")
        return None

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            # 소스와 타겟 모두 0건인 경우 경고
            if result and (result[0] == 0 and result[1] == 0):
                print(f"\n⚠️  [WARNING] {label}: 데이터가 0건입니다. dbt 모델의 변수 처리 로직을 확인하세요.")
            return result
    except SQLAlchemyError as e:
        print(f"\n🚨 [DB ERROR] {label} 실행 중 오류 발생: {e}")
        print(f"--- [Failed Query] ---\n{query}\n{'-'*50}")
        return None

# ==========================================
# 3. dbt 및 메타데이터 함수
# ==========================================
def get_compiled_sql_for_date(model_name, start_ts, end_ts):
    """
    매 루프마다 해당 날짜 구간을 변수로 주입하여 dbt compile 수행.
    이를 통해 모델 내부의 get_date_intervals()가 정확한 날짜를 리턴하게 함.
    """
    dbt_vars = {
        "data_interval_start": start_ts,
        "data_interval_end": end_ts
    }
    vars_arg = f" --vars '{json.dumps(dbt_vars)}'"
    
    cmd = f"dbt compile -q -s {model_name}{vars_arg}"
    try:
        sql = subprocess.check_output(cmd, shell=True, cwd=DBT_PROJECT_DIR, text=True)
        return sql.strip()
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt 컴파일 실패: {e.output}")
        return None

def get_target_meta(schema, table, col, engine, is_execute=True):
    """타겟 테이블의 메타데이터 확인"""
    if not is_execute:
        return 'timestamp', '%Y-%m-%d'

    type_query = f"SELECT data_type FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}' AND column_name = '{col}'"
    res = execute_query(type_query, engine, label="Metadata Check")
    if not res: return 'unknown', '%Y-%m-%d'
    
    col_type = res[0].lower()
    if 'timestamp' in col_type or 'date' in col_type: return 'timestamp', '%Y-%m-%d'
    
    sample_query = f"SELECT {col} FROM {schema}.{table} WHERE {col} IS NOT NULL LIMIT 1"
    sample = execute_query(sample_query, engine, label="Type Sampling")
    if sample and str(sample[0]).isdigit() and len(str(sample[0])) == 8: return 'yyyymmdd', '%Y%m%d'
    return 'varchar', '%Y-%m-%d'

# ==========================================
# 4. 메인 검증 실행 함수
# ==========================================
def audit_runner(full_model_name, date_col, start_date, end_date, is_execute=True):
    if '.' not in full_model_name:
        print("🚨 ERROR: 'schema.table' 형식을 사용하세요.")
        sys.exit(1)
        
    schema, model = full_model_name.split('.')
    engine = get_db_engine()
    
    # 타겟 메타데이터 확인 (1회)
    target_type, date_fmt = get_target_meta(schema, model, date_col, engine, is_execute=is_execute)
    
    results_buffer = []

    # [CASE 1] 'all' 모드
    if date_col.lower() == 'all' or start_date == 'all':
        # 전수 검사 시에는 변수 없이 컴파일
        base_sql = get_compiled_sql_for_date(model, "1900-01-01 00:00:00", "2099-12-31 23:59:59")
        query = f"SELECT (SELECT COUNT(*) FROM ({base_sql}) as src) as s_cnt, (SELECT COUNT(*) FROM {schema}.{model}) as t_cnt"
        
        if not is_execute:
            print(f"\n--- [Dry Run: Total Audit Query] ---\n{query}\n")
            return

        res = execute_query(query, engine, label=f"Total Audit: {model}")
        if res:
            diff = res[0] - res[1]
            results_buffer.append({'date': 'TOTAL', 's_cnt': res[0], 't_cnt': res[1], 'diff': diff, 'query': query})

    # [CASE 2] 일단위 루프 (컴파일 포함)
    else:
        curr = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        print(f"[*] Starting Daily Audit loop (Compiling each day)...")
        if not is_execute:
            print("[*] Mode: Dry Run (No database execution)")

        while curr <= end_dt:
            istart = curr.strftime('%Y-%m-%d 00:00:00')
            iend = (curr + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
            
            # 매번 해당 날짜에 맞춰 dbt 컴파일 (정확성 보장)
            compiled_sql = get_compiled_sql_for_date(model, istart, iend)
            if not compiled_sql:
                curr += timedelta(days=1)
                continue

            f_date = curr.strftime(date_fmt)
            target_where = f"{date_col} = '{f_date}'" if target_type == 'yyyymmdd' else f"{date_col} >= '{istart}' AND {date_col} < '{iend}'"
            
            # 소스 SQL은 이미 컴파일 시점에 날짜 필터가 걸려 있으므로 추가 WHERE 절 없이 COUNT 수행
            query = f"""
                WITH s AS (SELECT COUNT(*) as cnt FROM ({compiled_sql}) as src),
                     t AS (SELECT COUNT(*) as cnt FROM {schema}.{model} WHERE {target_where})
                SELECT s.cnt, t.cnt FROM s, t
            """
            
            if not is_execute:
                print(f"\n--- [Dry Run: {curr.date()}] ---\n{query}")
                curr += timedelta(days=1)
                continue

            res = execute_query(query, engine, label=f"Daily Audit: {curr.date()}", is_execute=is_execute)
            if res:
                diff = res[0] - res[1]
                results_buffer.append({'date': curr.strftime('%Y-%m-%d'), 's_cnt': res[0], 't_cnt': res[1], 'diff': diff, 'query': query})
                print(f"    - {curr.strftime('%Y-%m-%d')}: {'OK' if diff==0 else '🚨 DIFF(' + str(diff) + ')'}")
            
            curr += timedelta(days=1)

    if not is_execute:
        print("\n[*] Dry Run completed.")
        return

    # FINAL SUMMARY REPORT
    print(f"\n{'='*85}\n 📊 FINAL INTEGRITY SUMMARY: {full_model_name}\n{'='*85}")
    print(f"| {'Date':<10} | {'Source':>15} | {'Target':>15} | {'Diff':>10} | Status")
    print(f"|{'-'*12}|{'-'*17}|{'-'*17}|{'-'*12}|{'-'*10}")
    
    diff_queries = []
    for r in results_buffer:
        status = "✅ OK" if r['diff'] == 0 else "🚨 DIFF"
        print(f"| {r['date']:<10} | {r['s_cnt']:>15,} | {r['t_cnt']:>15,} | {r['diff']:>10,} | {status}")
        if r['diff'] != 0: diff_queries.append((r['date'], r['query']))

    if diff_queries:
        print(f"\n{'!'*85}\n 🔍 DEBUGGING QUERIES FOR DIFFERENCES\n{'!'*85}")
        for d_date, d_query in diff_queries:
            print(f"\n--- [Date: {d_date}] ---\n{d_query}\n{'-' * 50}")
    else:
        print("\n✨ All data is consistent. Integrity check passed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="dbt Integrity Auditor CLI")
    parser.add_argument("model", help="스키마.모델명 (예: marts.orders_mart)")
    parser.add_argument("column", help="날짜컬럼 또는 'all'")
    parser.add_argument("start", nargs='?', default="all", help="시작일 (YYYY-MM-DD)")
    parser.add_argument("end", nargs='?', default="all", help="종료일 (YYYY-MM-DD)")
    
    # Dry Run 옵션 추가 (is_execute 로직 대응)
    parser.add_argument("--dry-run", action="store_false", dest="is_execute", help="실제 쿼리를 실행하지 않고 SQL만 확인")
    parser.set_defaults(is_execute=True)

    args = parser.parse_args()
    audit_runner(args.model, args.column, args.start, args.end, is_execute=args.is_execute)