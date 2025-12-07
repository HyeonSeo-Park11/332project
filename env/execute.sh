#!/bin/bash

# 1. Argument 파싱 (workDir, hostFile, workerNum)
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <workDir> <hostFile> <workerNum>"
    exit 1
fi

WORK_DIR=$1
HOST_FILE=$2
NUMBER=$3

OUTPUT_DIR="output"
pssh -h "$HOST_FILE" -i "rm -rf \$(pwd)/$OUTPUT_DIR"

# HOST_FILE 경로를 절대 경로로 변환 (cd 명령어로 이동 후에도 파일 위치를 찾기 위함)
HOST_FILE=$(realpath "$HOST_FILE")

echo "========================================"
echo "Job Start"
echo "WorkDir: $WORK_DIR"
echo "HostFile: $HOST_FILE"
echo "Number: $NUMBER"
echo "========================================"

# 2. WorkDir로 이동 후 특정 명령어 실행
echo "[Step 2] Moving to directory and running setup command..."
cd "$WORK_DIR"

# 3. pscp를 통해 특정 파일 전송
echo "[Step 3] Sending file via pscp..."

LOCAL_FILE="worker/target/scala-2.13/worker.jar"      # 보낼 파일
REMOTE_DEST="/home/cyan"              # 원격지 저장 경로
# ---------------------------------------------------------
pscp -h "$HOST_FILE" "$LOCAL_FILE" "$REMOTE_DEST"


# 4. 명령어 실행(number 사용) -> 첫 줄 저장 -> pssh 실행
echo "[Step 4] Generating token and executing pssh..."

CMD="java -jar master/target/scala-2.13/master.jar"

# 메인 명령어 실행 결과를 파이프(|)로 받아 처리
$CMD "$NUMBER" | {
    # (1) 딱 한 줄만 읽음
    if IFS= read -r FIRST_LINE; then
        # (2) 읽은 줄은 화면에 그대로 출력 (사용자 요청: 출력 유지)
        echo "$FIRST_LINE"

        # (3) 첫 줄을 이용해 PSSH를 '백그라운드(&)'로 실행
        #     이렇게 하면 아래쪽 while 루프가 도는 동안 pssh도 동시에 돕니다.
        (
            echo "   [Trigger] First line detected. Launching PSSH in background..."
            # [수정 필요] 호스트에서 실행할 명령어
	    JAVA_PATH="/home/cyan/.local/java/jdk-17/bin/java"
            pssh -h "$HOST_FILE" -t 0 -i "time $JAVA_PATH -jar worker.jar $FIRST_LINE -I /dataset/small -O \$(pwd)/$OUTPUT_DIR"
        ) & 
    fi

    # (4) 나머지 줄이 있다면 계속 읽어서 화면에 출력 (명령어가 끝날 때까지)
    while IFS= read -r line; do
        echo "$line"
    done
}

# 5. 종료 처리 (pssh가 끝나면 실행)
echo "[Step 5] All remote commands finished."

SLEEP_SEC=5
echo "Waiting for $SLEEP_SEC seconds before exiting..."
sleep $SLEEP_SEC

echo "[Verify 1] Checking Records count and Success status..."

# PSSH 결과물을 저장할 임시 디렉터리 생성
LOG_DIR=$(mktemp -d)
# 각 호스트의 결과 파일($REMOTE_RESULT_FILE) 내용을 가져옴 (`cat` 명령 사용)
REMOTE_RESULT_FILE="partition.*"
pssh -h "$HOST_FILE" -o "$LOG_DIR" "valsort \$(pwd)/$OUTPUT_DIR/$REMOTE_RESULT_FILE 2>&1" > /dev/null 2>&1

TOTAL_RECORDS=0
IS_SUCCESS=true

# host 파일에 있는 목록 순서대로(혹은 디렉터리 내 파일 순회) 검사
for log_file in "$LOG_DIR"/*; do
    # 1-1. SUCCESS 검사
    if ! grep -q "SUCCESS" "$log_file"; then
        echo "Error: SUCCESS message not found in $(basename "$log_file")"
        IS_SUCCESS=false
    fi

    # 1-2. Records 파싱 (Records: 4021353 형태)
    # awk로 'Records:' 다음 숫자를 가져옴
    REC=$(grep "Records:" "$log_file" | awk '{print $2}')
    
    if [ -z "$REC" ]; then
        echo "Error: Cannot parse Records number in $(basename "$log_file")"
        REC=0
    fi
    
    TOTAL_RECORDS=$((TOTAL_RECORDS + REC))
done

# 1-3. 검증
DATASET_NUMBER=640000
EXPECTED_TOTAL=$((DATASET_NUMBER * NUMBER))

echo "  - Total Records: $TOTAL_RECORDS (Expected: $EXPECTED_TOTAL)"

if [ "$IS_SUCCESS" = false ]; then
    echo "  [FAIL] Some hosts did not report SUCCESS."
    exit 1
elif [ "$TOTAL_RECORDS" -ne "$EXPECTED_TOTAL" ]; then
    echo "  [FAIL] Total records mismatch!"
    exit 1
else
    echo "  [PASS] All hosts SUCCESS and Records count matches."
fi


# ---------------------------------------------------------
# [신규 로직] 2. Head/Tail 순서 검사 (Distributed Sort Check)
# ---------------------------------------------------------
echo "[Verify 2] Checking Distributed Sort Order (Head/Tail)..."

SORT_CHECK_DIR=$(mktemp -d)

# 각 호스트의 Head(첫 10바이트), Tail(첫 10바이트) 가져오기
# head -n 1으로 첫 줄을 따고, head -c 10으로 10바이트 자름
pssh -h "$HOST_FILE" -o "$SORT_CHECK_DIR" \
    "head -c 10 \$(pwd)/$OUTPUT_DIR/$REMOTE_RESULT_FILE | xxd -p -u; tail -c 100 \$(pwd)/$OUTPUT_DIR/$REMOTE_RESULT_FILE | head -c 10 | xxd -p -u;" > /dev/null 2>&1

PREV_TAIL=""
FIRST_HOST=true

# host 파일에 적힌 순서대로 순회해야 함 (매우 중요)
while IFS= read -r host || [ -n "$host" ]; do
    # pssh output 파일명은 호스트명과 일치함
    # (포트가 포함된 경우 파일명 처리가 다를 수 있으니 주의, 여기선 기본 호스트명 가정)
    OUTPUT_FILE="$SORT_CHECK_DIR/$host"

    if [ ! -f "$OUTPUT_FILE" ]; then
        echo "Warning: Output file for $host not found. Skipping."
        continue
    fi

    # 파일의 첫 줄(Head), 두 번째 줄(Tail) 읽기
    # mapfile은 bash 4.0 이상, 안전하게 read 사용
    CURR_HEAD=$(sed -n '1p' "$OUTPUT_FILE")
    CURR_TAIL=$(sed -n '2p' "$OUTPUT_FILE")

    PRETTY_PREV_TAIL=$(python3 -c "print(str(bytes.fromhex('$PREV_TAIL'))[1:])")
    PRETTY_HEAD=$(python3 -c "print(str(bytes.fromhex('$CURR_HEAD'))[1:])")
    PRETTY_TAIL=$(python3 -c "print(str(bytes.fromhex('$CURR_TAIL'))[1:])")

    echo "  - Checking $host: Head=$PRETTY_HEAD, Tail=$PRETTY_TAIL"

    if [ "$FIRST_HOST" = true ]; then
        PREV_TAIL="$CURR_TAIL"
        FIRST_HOST=false
        continue
    fi

    # 문자열 비교: 이전 호스트의 Tail이 현재 호스트의 Head보다 작거나 같아야 함
    # (사전순 정렬 기준)
    if [[ "$PREV_TAIL" > "$CURR_HEAD" ]]; then
        echo "  [FAIL] Sort order broken between hosts!"
        echo "         Previous Tail: $PRETTY_PREV_TAIL"
        echo "         Current Head : $PRETTY_HEAD"
        exit 1
    fi

    PREV_TAIL="$CURR_TAIL"

done < "$HOST_FILE"

echo "  [PASS] Distributed sort order verified."

# 임시 디렉터리 정리
rm -rf "$LOG_DIR" "$SORT_CHECK_DIR"

echo "================ Job Finished Successfully ================"
