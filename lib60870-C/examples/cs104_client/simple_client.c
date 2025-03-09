#include "cs104_connection.h"
#include "hal_time.h"
#include "hal_thread.h"

#include <stdio.h>
#include <stdlib.h>

#include <time.h>

#include <pthread.h>
#include <sched.h>

#define QUEUE_SIZE 10000
#define WORKER_THREADS 4
#define MAX_ASDU_TYPES 32
#define MAX_IOA_COUNT 65536

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
    Semaphore full;
    Semaphore empty;
} RingBuffer;

typedef struct
{
    int asdu_count[MAX_ASDU_TYPES];               // 统计每个ASDU类型的数量
    int ioa_count[MAX_ASDU_TYPES][MAX_IOA_COUNT]; // 每个ASDU类型下的每个IOA的数量
    pthread_mutex_t lock;                         // 用于线程同步
} ASDU_Stats;

// 计时器结构体，保存开始和结束时间
typedef struct {
    clock_t start;
    clock_t end;
} Timer;

ASDU_Stats asduStats;  // 全局统计变量
RingBuffer asduQueue;

void ringBufferInit(RingBuffer *rb)
{
    rb->head = rb->tail = rb->count = 0;
    pthread_mutex_init(&rb->lock, NULL);
    rb->full = Semaphore_create(0);
    rb->empty = Semaphore_create(QUEUE_SIZE);
}

void enqueue(RingBuffer *rb, CS101_ASDU *asdu)
{
    Semaphore_wait(rb->empty);
    pthread_mutex_lock(&rb->lock);

    rb->buffer[rb->tail].asdu = asdu;
    rb->tail = (rb->tail + 1) % QUEUE_SIZE;
    rb->count++;

    pthread_mutex_unlock(&rb->lock);
    Semaphore_post(rb->full);
}

CS101_ASDU *dequeue(RingBuffer *rb)
{
    Semaphore_wait(rb->full);
    pthread_mutex_lock(&rb->lock);

    CS101_ASDU *asdu = rb->buffer[rb->head].asdu;
    rb->head = (rb->head + 1) % QUEUE_SIZE;
    rb->count--;

    pthread_mutex_unlock(&rb->lock);
    Semaphore_post(rb->empty);
    return asdu;
}

// 启动计时器
void startTimer(Timer* timer) {
    timer->start = clock();
}

// 停止计时器并打印时间差（毫秒）
void stopTimer(Timer* timer) {
    timer->end = clock();
    double duration = ((double)(timer->end - timer->start)) / CLOCKS_PER_SEC * 1000; // 毫秒
    printf("Function execution time: %.3f milliseconds\n", duration);
}

void updateStats(CS101_ASDU asdu)
{
    int typeID = CS101_ASDU_getTypeID(asdu);  // 获取ASDU类型
    int numElements = CS101_ASDU_getNumberOfElements(asdu);  // 获取该类型下的元素数量

    // 加锁，确保线程安全
    pthread_mutex_lock(&asduStats.lock);

    // 确保类型ID在合理范围内
    if (typeID < 0 || typeID >= MAX_ASDU_TYPES) {
        pthread_mutex_unlock(&asduStats.lock);
        return;  // 如果类型ID不合法，直接返回
    }

    // 对应类型的ASDU计数增加
    asduStats.asdu_count[typeID]++;

    // 统计该类型下的每个IOA点位
    for (int i = 0; i < numElements; i++)
    {
        InformationObject io = CS101_ASDU_getElement(asdu, i);  // 获取元素
        if (io != NULL)
        {
            int ioa = InformationObject_getObjectAddress(io);  // 获取IOA地址
            if (ioa >= 0 && ioa < MAX_IOA_COUNT)  // 确保IOA地址合法
            {
                // 根据ASDU类型统计对应的IOA点位
                asduStats.ioa_count[typeID][ioa]++;
            }
        }
    }

    // 解锁
    pthread_mutex_unlock(&asduStats.lock);
}


void workerThreadFunction(void *arg)
{
    while (1)
    {
        CS101_ASDU *asdu = dequeue(&asduQueue);
        if (asdu)
        {
            updateStats(asdu);
        }
        CS101_ASDU_destroy(asdu);
    }
}

char* formatTimestamp() {
    time_t rawtime;
    struct tm *timeinfo;

    // 获取当前时间戳
    time(&rawtime);
    timeinfo = localtime(&rawtime);

    // 为返回的字符串动态分配内存
    char *timestamp = (char*)malloc(20 * sizeof(char)); // 格式: "YYYY-MM-DD HH:MM:SS"
    if (timestamp != NULL) {
        // 格式化时间为：年-月-日 时:分:秒
        strftime(timestamp, 20, "%Y-%m-%d %H:%M:%S", timeinfo);
    }

    return timestamp;
}

void statsThreadFunction(void *arg)
{
    while (1)
    {
        Thread_sleep(1000);
        pthread_mutex_lock(&asduStats.lock);

        for (int i = 0; i < MAX_ASDU_TYPES; i++)
        {
            if (asduStats.asdu_count[i] > 0)
            {
                // printf("ASDU 类型 %d: %d 个\n", i, asduStats.asdu_count[i]);
                
                int ioa_count = 0;
                // 打印每种ASDU类型下的IOA统计
                for (int j = 0; j < MAX_IOA_COUNT; j++)
                {
                    if (asduStats.ioa_count[i][j] > 0)
                    {   // 统计每一种IOA有多少个点位 IOA 8: 1 次
                        // printf("\tIOA %d: %d 次\n", j, asduStats.ioa_count[i][j]);
                        ioa_count ++;
                    }
                }

                printf("%s ASDU 类型 %s(%d): %d 个\n", formatTimestamp(), TypeID_toString(i), i, ioa_count);
            }
        }

        // 重置统计
        memset(&asduStats, 0, sizeof(ASDU_Stats));

        pthread_mutex_unlock(&asduStats.lock);
    }
}


void initThreads()
{
    ringBufferInit(&asduQueue);
    pthread_mutex_init(&asduStats.lock, NULL);
    memset(&asduStats, 0, sizeof(asduStats));

    for (int i = 0; i < WORKER_THREADS; i++)
    {
        Thread workerThread = Thread_create(workerThreadFunction, NULL, false);
        Thread_start(workerThread);
    }

    Thread statsThread = Thread_create(statsThreadFunction, NULL, false);
    Thread_start(statsThread);
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

int main(int argc, char **argv)
{
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
        port = atoi(argv[4]);

    initThreads();

    printf("Connecting to: %s:%i\n", ip, port);
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

#if 0
        InformationObject sc = (InformationObject)
                SingleCommand_create(NULL, 5000, true, false, 0);

        // 遥控单点命令 C_SC_NA_1 
        printf("Send control command C_SC_NA_1\n");
        CS104_Connection_sendProcessCommandEx(con, CS101_COT_ACTIVATION, 1, sc);

        InformationObject_destroy(sc);

        /* Send clock synchronization command */
        struct sCP56Time2a newTime;

        CP56Time2a_createFromMsTimestamp(&newTime, Hal_getTimeInMs());

        printf("Send time sync command\n");
        CS104_Connection_sendClockSyncCommand(con, 1, &newTime);
#endif

        // **添加循环，让程序持续运行，直到用户输入 'q'**
        char input[10];
        while (true)
        {
            printf("> ");
            fflush(stdout);
            if (fgets(input, sizeof(input), stdin))
            {
                if (input[0] == 'q' && (input[1] == '\n' || input[1] == '\0'))
                {
                    printf("Exiting...\n");
                    break;
                }
            }
            Thread_sleep(1000);
        }
    }
    else
        printf("Connect failed!\n");

    Thread_sleep(1000);

    CS104_Connection_destroy(con);

    printf("exit\n");
}
