WSL 기반 dbt 교육 환경 설정 가이드

이 문서는 WSL(Windows Subsystem for Linux) 환경에서 dbt 교육 프로젝트(edu)를 설정하기 위한 단계별 가이드입니다.

1. Python 3.12 및 필수 라이브러리 설치

WSL(Ubuntu/Debian) 환경에서 Python 3.12와 dbt-postgres 등 라이브러리 컴파일에 필요한 필수 패키지를 설치합니다.

sudo apt update && sudo apt upgrade -y
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update

# Python 3.12 및 venv, dev, pip 설치
sudo apt install python3.12 python3.12-venv python3.12-dev python3-pip -y

# 기본 Python 버전을 3.12로 설정
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1
sudo update-alternatives --config python3


설치 후 버전을 확인합니다.

python3 --version


2. 가상 환경 (venv) 설정

프로젝트 종속성을 격리하고 시스템 라이브러리와의 충돌을 방지하기 위해 가상 환경을 사용합니다.

프로젝트 디렉토리 이동: cd ~/projects/edu (본인의 경로에 맞게 수정)

가상 환경 생성:

python3 -m venv .venv


가상 환경 활성화:

source .venv/bin/activate


프롬프트 왼쪽에 (.venv) 표시가 나타나는지 확인합니다.

3. 필수 패키지 설치

활성화된 가상 환경 내에서 dbt 및 관련 라이브러리를 설치합니다.

pip 최신화:

pip install --upgrade pip


패키지 설치:

pip install -r refs/requirements.txt


4. 데이터베이스 연결 설정 (dbconf.json)

dbconf.json.template 파일을 기반으로 실제 접속 정보를 설정합니다.

파일 복사:

cp refs/edu/dbconf.json.template refs/edu/dbconf.json


정보 수정: refs/edu/dbconf.json 파일을 열어 본인에게 할당된 DB 접속 정보(Host, Port, User, Password 등)를 입력합니다.

보안 주의: dbconf.json에는 비밀번호가 포함되어 있으므로, 반드시 .gitignore에 등록되어 있는지 확인하십시오.

5. 초기 스키마 및 원천 데이터 구성 (Seeding)

실습에 필요한 원천 테이블을 생성하고 데이터를 로드합니다.

스키마 초기화 (edu, stg, marts 삭제 및 생성):

python3 tools/manage_schemas_for_test.py


테이블 생성 (DDL):

python3 tools/execute_all_ddls.py


샘플 데이터 로드 (CSV -> DB):

python3 tools/load_data_from_csv.py


6. 가공 영역 (stg, marts) 테이블 사전 생성

dbt 실행 전, 모델이 들어갈 스키마 구조를 미리 생성합니다.

전체 DDL 재실행:

python3 tools/execute_all_ddls.py


7. dbt 환경 설정 확인 (사전 참고)

dbt의 핵심 설정 파일인 profiles.yml은 기본적으로 아래 경로에서 관리됩니다. 교육 세션 중에 상세 설정을 진행할 예정이니 경로를 미리 기억해 두십시오.

경로: ~/.dbt/profiles.yml

디렉토리 생성: (미리 생성해 두면 편리합니다)

mkdir -p ~/.dbt


✅ 설치 완료 체크리스트

[ ] python3 --version 결과가 3.12.x 인가?

[ ] 터미널 프롬프트에 (.venv) 가 표시되어 있는가?

[ ] dbconf.json에 본인의 DB 접속 정보가 정확히 입력되었는가?

[ ] edu 스키마 내 테이블들에 데이터가 정상적으로 들어있는가? (SQL 툴로 확인)