#include "cs104_connection.h"
#include "hal_time.h"
#include "hal_thread.h"

#include <stdio.h>
#include <stdlib.h>

#include <time.h>

#include <pthread.h>
#include <sched.h>

#include <semaphore.h>
#include "version.h"

#define QUEUE_SIZE 10000
#define WORKER_THREADS 4
#define MAX_ASDU_TYPES 64
#define MAX_IOA_COUNT 65535
#define LOG_COUNT_MAX 12000
#define TIMESTAMP_SIZE 25

#define TYPE_COMMAND_YK 1
#define TYPE_COMMAND_YT 2

static int logCount = 0;
static bool DEBUG_LOG_SWITCH = false;

static bool isLogIOA = true;

typedef struct
{
    CS101_ASDU asdu;
} ASDU_Item;

typedef struct
{
    ASDU_Item buffer[QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
} RingBuffer;

typedef struct
{
    int asdu_count[MAX_ASDU_TYPES];               // 统计每个ASDU类型的数量
    int ioa_count[MAX_ASDU_TYPES][MAX_IOA_COUNT]; // 每个ASDU类型下的每个IOA的数量
} ASDU_Stats;

static pthread_mutex_t mAsduStatsLock; // 用于线程同步

// 计时器结构体，保存开始和结束时间
typedef struct
{
    clock_t start;
    clock_t end;
} Timer;

ASDU_Stats *asduStats; // 全局统计变量
RingBuffer asduQueue;

Thread mStatsThread; // 统计线程
Thread mWorkThreadArr[WORKER_THREADS];

void ringBufferInit(RingBuffer *rb)
{
    rb->head = rb->tail = 0;

    __atomic_store_n(&rb->count, 0, __ATOMIC_RELAXED); // 初始化原子变量

    pthread_mutex_init(&rb->lock, NULL);
}

void enqueue(RingBuffer *rb, CS101_ASDU *asdu)
{
    if (__atomic_load_n(&rb->count, __ATOMIC_ACQUIRE) >= QUEUE_SIZE)
    {
        __atomic_fetch_add(&logCount, 1, __ATOMIC_RELEASE);
        if (__atomic_load_n(&logCount, __ATOMIC_ACQUIRE) >= LOG_COUNT_MAX)
        {
            if (DEBUG_LOG_SWITCH)
            {
                printf("Trying to enqueue, enqueue is full : %d\n", __atomic_load_n(&rb->count, __ATOMIC_ACQUIRE));
            }
            __atomic_store_n(&logCount, 0, __ATOMIC_SEQ_CST);
        }

        // 释放asdu的内存
        CS101_ASDU_destroy(asdu);
        return;
    }

    pthread_mutex_lock(&rb->lock);

    if (__atomic_load_n(&rb->count, __ATOMIC_ACQUIRE) < QUEUE_SIZE)
    {
        rb->buffer[rb->tail].asdu = asdu;
        rb->tail = (rb->tail + 1) % QUEUE_SIZE;
        __atomic_fetch_add(&rb->count, 1, __ATOMIC_RELEASE);
    }

    if (DEBUG_LOG_SWITCH)
    {
        // printf("enqueue success => count %d\n", __atomic_load_n(&rb->count, __ATOMIC_ACQUIRE));
    }

    pthread_mutex_unlock(&rb->lock);
}

CS101_ASDU *dequeue(RingBuffer *rb)
{
    if (__atomic_load_n(&rb->count, __ATOMIC_ACQUIRE) == 0)
    {
        __atomic_fetch_add(&logCount, 1, __ATOMIC_RELEASE);
        if (__atomic_load_n(&logCount, __ATOMIC_ACQUIRE) >= LOG_COUNT_MAX)
        {
            if (DEBUG_LOG_SWITCH)
            {
                printf("Trying to dequeue, enqueue is empty : %d\n", __atomic_load_n(&rb->count, __ATOMIC_ACQUIRE));
            }
            __atomic_store_n(&logCount, 0, __ATOMIC_SEQ_CST);
        }

        Thread_sleep(20);
        return NULL;
    }

    pthread_mutex_lock(&rb->lock);

    void *asdu = NULL;

    if (__atomic_load_n(&rb->count, __ATOMIC_ACQUIRE) > 0)
    {
        asdu = rb->buffer[rb->head].asdu;
        rb->head = (rb->head + 1) % QUEUE_SIZE;
        __atomic_fetch_sub(&rb->count, 1, __ATOMIC_RELEASE); // 原子递减
    }

    if (DEBUG_LOG_SWITCH)
    {
        printf("dequeue success => count %d\n", __atomic_load_n(&rb->count, __ATOMIC_ACQUIRE));
    }

    pthread_mutex_unlock(&rb->lock);

    return asdu;
}

// 启动计时器
void startTimer(Timer *timer)
{
    timer->start = clock();
}

// 停止计时器并打印时间差（毫秒）
void stopTimer(Timer *timer)
{
    timer->end = clock();
    double duration = ((double)(timer->end - timer->start)) / CLOCKS_PER_SEC * 1000; // 毫秒
    printf("Function execution time: %.3f milliseconds\n", duration);
}

void updateStats(CS101_ASDU asdu)
{
    int typeID = CS101_ASDU_getTypeID(asdu);                // 获取ASDU类型
    int numElements = CS101_ASDU_getNumberOfElements(asdu); // 获取该类型下的元素数量

    if (typeID < 0 || typeID >= MAX_ASDU_TYPES)
    {
        return; // 如果类型ID不合法，直接返回
    }

    pthread_mutex_lock(&mAsduStatsLock);

    // 对应类型的ASDU计数增加
    asduStats->asdu_count[typeID]++;

    // 统计该类型下的每个IOA点位
    for (int i = 0; i < numElements; i++)
    {
        InformationObject io = CS101_ASDU_getElement(asdu, i); // 获取元素
        if (io != NULL)
        {
            int ioa = InformationObject_getObjectAddress(io); // 获取IOA地址
            if (ioa >= 0 && ioa < MAX_IOA_COUNT)              // 确保IOA地址合法
            {
                // 根据ASDU类型统计对应的IOA点位
                asduStats->ioa_count[typeID][ioa]++;
            }
        }
        InformationObject_destroy(io);
    }

    pthread_mutex_unlock(&mAsduStatsLock);
}

void workerThreadFunction(void *arg)
{
    while (true)
    {
        CS101_ASDU *asdu = dequeue(&asduQueue);
        if (asdu)
        {
            updateStats(asdu);
            CS101_ASDU_destroy(asdu);
        }
    }
}

void formatTimestamp(char *timestamp, int size) {
    struct timeval tv;
    struct tm *timeinfo;

    gettimeofday(&tv, NULL);  // 获取当前时间（秒+微秒）
    timeinfo = localtime(&tv.tv_sec);  // 转换为本地时间

    // 格式化时间到秒
    int len = strftime(timestamp, size, "%Y-%m-%d %H:%M:%S", timeinfo);
    
    // 追加毫秒部分
    if (len > 0 && len < size) {
        snprintf(timestamp + len, size - len, ".%03ld", tv.tv_usec / 1000);
    }
}

void statsThreadFunction(void *arg)
{
    while (true)
    {
        pthread_mutex_lock(&mAsduStatsLock);
        u_int16_t ioaCount = 0;

        ASDU_Stats *localStats = (ASDU_Stats *)malloc(sizeof(ASDU_Stats));
        if (localStats == NULL)
        {
            printf("localStats Memory allocation failed!\n");
            pthread_mutex_unlock(&mAsduStatsLock);

            Thread_sleep(200);
            continue;
        }

        memcpy(localStats, asduStats, sizeof(ASDU_Stats));
        memset(asduStats, 0, sizeof(ASDU_Stats));
        char timestampString[TIMESTAMP_SIZE];
        formatTimestamp(timestampString, TIMESTAMP_SIZE);

        for (int i = 0; i < MAX_ASDU_TYPES; i++)
        {
            if (localStats->asdu_count[i] > 0)
            {
                int ioa_count = 0;
                // 打印每种ASDU类型下的IOA统计
                for (int j = 0; j < MAX_IOA_COUNT; j++)
                {
                    if (localStats->ioa_count[i][j] > 0)
                    {
                        ioa_count++;
                        ioaCount++;
                    }
                }

                if (isLogIOA)
                    printf("%s ASDU(%d) TypeID %s(%d): %d 个\n", timestampString, localStats->asdu_count[i], TypeID_toString(i), i, ioa_count);
            }
        }

        if (isLogIOA)
            printf("IOA总点数: %d\n", ioaCount);

        free(localStats); // 释放本地统计数据
        pthread_mutex_unlock(&mAsduStatsLock);

        Thread_sleep(1000);
    }
}

void initThreads()
{
    ringBufferInit(&asduQueue);
    pthread_mutex_init(&mAsduStatsLock, NULL);

    asduStats = (ASDU_Stats *)malloc(sizeof(ASDU_Stats));
    if (asduStats == NULL)
    {
        printf("Memory allocation failed!\n");
        return;
    }
    memset(asduStats, 0, sizeof(ASDU_Stats));

    for (int i = 0; i < WORKER_THREADS; i++)
    {
        mWorkThreadArr[i] = Thread_create(workerThreadFunction, NULL, false);
        if (mWorkThreadArr[i] != NULL)
        {
            Thread_start(mWorkThreadArr[i]);
        }
    }

    mStatsThread = Thread_create(statsThreadFunction, NULL, false);
    if (mStatsThread != NULL)
    {
        Thread_start(mStatsThread);
    }
}

/* Callback handler to log sent or received messages (optional) */
static void
rawMessageHandler(void *parameter, uint8_t *msg, int msgSize, bool sent)
{
    if (sent)
        printf("SEND: ");
    else
        printf("RCVD: ");

    int i;
    for (i = 0; i < msgSize; i++)
    {
        printf("%02x ", msg[i]);
    }

    printf("\n");
}

/* Connection event handler */
static void
connectionHandler(void *parameter, CS104_Connection connection, CS104_ConnectionEvent event)
{
    switch (event)
    {
    case CS104_CONNECTION_OPENED:
        printf("Connection established\n");
        break;
    case CS104_CONNECTION_CLOSED:
        printf("Connection closed\n");
        break;
    case CS104_CONNECTION_FAILED:
        printf("Failed to connect\n");
        break;
    case CS104_CONNECTION_STARTDT_CON_RECEIVED:
        printf("Received STARTDT_CON\n");
        break;
    case CS104_CONNECTION_STOPDT_CON_RECEIVED:
        printf("Received STOPDT_CON\n");
        break;
    }
}

/*
 * CS101_ASDUReceivedHandler implementation
 * For CS104 the address parameter has to be ignored
 */
// ASDU 回包处理，比如突变上送的值。
static bool
asduReceivedHandler(void *parameter, int address, CS101_ASDU asdu)
{
    IEC60870_5_TypeID typeId = CS101_ASDU_getTypeID(asdu);
    if (typeId == C_SC_NA_1 || typeId == C_SE_NB_1){
        char timestampString[TIMESTAMP_SIZE];
        formatTimestamp(timestampString, TIMESTAMP_SIZE);

        printf("%s Recv 遥调或者遥控返回 %s(%d)!\n",timestampString, TypeID_toString(typeId), typeId);
    }else {
        CS101_ASDU *asduCopy = CS101_ASDU_clone(asdu, NULL);
        if (asduCopy)
        {
            enqueue(&asduQueue, asduCopy);
        }
        else
        {
            printf("ASDU copy failed!\n");
        }
    }
}

// 遥信控制
void Send_Command(CS104_Connection *con, int type, int coa, int ioa, int command, bool selectCommand)
{ 
    InformationObject sc = NULL;

    char timestampString[TIMESTAMP_SIZE];
    formatTimestamp(timestampString, TIMESTAMP_SIZE);

    printf("%s Send control command C_SC_NA_1 coa=%d ,ioa=%d ,command=%d ,selectCommand=%d\n",
           timestampString, coa, ioa, command, selectCommand);

    if (type == TYPE_COMMAND_YK)
    {

        // 遥控单点命令 C_SC_NA_1
        sc = (InformationObject)SingleCommand_create(NULL, ioa, (command == 1 ? true : false), selectCommand, 0);
    }
    else if (type == TYPE_COMMAND_YT)
    {
        // 遥测浮点
        sc = (InformationObject)SetpointCommandScaled_create(NULL, ioa, command, selectCommand, 0);
    }
    CS104_Connection_sendProcessCommandEx(con, CS101_COT_ACTIVATION, coa, sc);

    InformationObject_destroy(sc);
}

// ./simple_client <server_ip> <server_port> <local_ip> <local_port>
// local_ip和local_port 非必填项
int main(int argc, char **argv)
{
    print_version();

    const char *ip = "localhost";                 // 从站IP地址
    uint16_t port = IEC_60870_5_104_DEFAULT_PORT; // 从站端口
    const char *localIp = NULL;                   // 本地IP地址
    int localPort = -1;

    if (argc > 1)
        ip = argv[1];

    if (argc > 2)
        port = atoi(argv[2]);

    if (argc > 3)
        localIp = argv[3];

    if (argc > 4)
        localPort = atoi(argv[4]);

    printf("Connecting to: %s:%i; local_ip: %s, local_port:%d \n", ip, port, localIp, localPort);
    CS104_Connection con = CS104_Connection_create(ip, port);

    CS101_AppLayerParameters alParams = CS104_Connection_getAppLayerParameters(con);
    alParams->originatorAddress = 3;

    CS104_Connection_setConnectionHandler(con, connectionHandler, NULL);
    CS104_Connection_setASDUReceivedHandler(con, asduReceivedHandler, NULL);

    /* optional bind to local IP address/interface */
    if (localIp)
        CS104_Connection_setLocalAddress(con, localIp, localPort);

    /* uncomment to log messages */
    // CS104_Connection_setRawMessageHandler(con, rawMessageHandler, NULL);

    if (CS104_Connection_connect(con))
    {
        printf("Connected!\n");

        initThreads();

        // 启动数据传输 Data Transfer
        CS104_Connection_sendStartDT(con);

        Thread_sleep(2000);

        // 召唤限定词 IEC60870_QOI_STATION 站级总召唤
        CS104_Connection_sendInterrogationCommand(con, CS101_COT_ACTIVATION, 1, IEC60870_QOI_STATION);

        Thread_sleep(5000);

        struct sCP56Time2a testTimestamp;
        CP56Time2a_createFromMsTimestamp(&testTimestamp, Hal_getTimeInMs());

        // 测试帧带时标
        CS104_Connection_sendTestCommandWithTimestamp(con, 1, 0x4938, &testTimestamp);

        // **添加循环，让程序持续运行，直到用户输入 'q'**
        char input[100];
        while (true)
        {
            printf("> ");
            fflush(stdout);

            if (fgets(input, sizeof(input), stdin))
            {
                // 去掉换行符
                input[strcspn(input, "\n")] = '\0';

                // 检查是否输入 'q' 退出
                if (strcmp(input, "q") == 0)
                {
                    printf("Exiting...\n");
                    break;
                }
                else if (strcmp(input, "d") == 0)
                {
                    // 是否展示IOA统计
                    isLogIOA = !isLogIOA;

                    if (isLogIOA){
                        printf("打开IOA统计...\n");
                    }else{
                        printf("关闭IOA统计...\n");
                    }
                    continue;
                }

                // 解析输入
                char command[30];
                int param1, param2;
                int count = sscanf(input, "%s %d %d", command, &param1, &param2);

                if (count > 0)
                {
                    if (strcmp(command, "yk") == 0 && count == 3)
                    {
                        Send_Command(con, TYPE_COMMAND_YK, 1, param1, param2, false);
                    }
                    else if (strcmp(command, "yt") == 0 && count == 3)
                    {
                        Send_Command(con, TYPE_COMMAND_YT, 1, param1, param2, false);
                    }
                    else
                    {
                        printf("Unknown command or invalid parameters\n");
                    }
                }
            }
        }
    }
    else
        printf("Connect failed!\n");

    Thread_sleep(1000);

    CS104_Connection_destroy(con);

    // 释放资源
    pthread_mutex_destroy(&mAsduStatsLock);
    if (asduStats != NULL)
    {
        free(asduStats);
        asduStats = NULL;
    }

    if (mStatsThread != NULL)
    {
        Thread_destroy(mStatsThread);
        mStatsThread == NULL;
    }

    for (int i = 0; i < WORKER_THREADS; i++)
    {
        if (mWorkThreadArr[i] != NULL)
        {
            Thread_destroy(mWorkThreadArr[i]);
        }
    }

    printf("==退出==\n");
}
