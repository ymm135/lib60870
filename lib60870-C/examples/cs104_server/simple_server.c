#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>

#include "cs104_slave.h"

#include "hal_thread.h"
#include "hal_time.h"

#include <getopt.h>
#include "simple_server.h"

#include <time.h>

#define YX_NUM_DEFAULT 10
#define YC_NUM_DEFAULT 10
#define UPDATE_GAP_DEFAULT 1
#define IOA_MERGE_NUM 40 // 合并的点个数

#define TYPE_YX 1        // 遥信
#define TYPE_YC (1 << 2) // 遥测

static bool running = true;
static bool mYxScaledValue = 1;
static int16_t mYcScaledValue = 1;
static int mYxNum = YX_NUM_DEFAULT;
static int mYcNum = YC_NUM_DEFAULT;
static bool mIOAMerge = false;
static int mUpdateSecond = false; // 更新频率
static bool mIsStopAutoUpdateIOA = false;

Thread mUpdateThread = NULL;
time_t currentTime;

void sigint_handler(int signalId)
{
    running = false;
}

void printCP56Time2a(CP56Time2a time)
{
    printf("%02i:%02i:%02i %02i/%02i/%04i", CP56Time2a_getHour(time),
           CP56Time2a_getMinute(time),
           CP56Time2a_getSecond(time),
           CP56Time2a_getDayOfMonth(time),
           CP56Time2a_getMonth(time),
           CP56Time2a_getYear(time) + 2000);
}

/* Callback handler to log sent or received messages (optional) */
static void
rawMessageHandler(void *parameter, IMasterConnection conneciton, uint8_t *msg, int msgSize, bool sent)
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

static bool
clockSyncHandler(void *parameter, IMasterConnection connection, CS101_ASDU asdu, CP56Time2a newTime)
{
    printf("Process time sync command with time ");
    printCP56Time2a(newTime);
    printf("\n");

    uint64_t newSystemTimeInMs = CP56Time2a_toMsTimestamp(newTime);

    /* Set time for ACT_CON message */
    CP56Time2a_setFromMsTimestamp(newTime, Hal_getTimeInMs());

    /* update system time here */

    return true;
}

// 总召处理
static bool
interrogationHandler(void *parameter, IMasterConnection connection, CS101_ASDU asdu, uint8_t qoi)
{
    printf("Received interrogation for group %i\n", qoi);

    if (qoi == 20)
    { /* only handle station interrogation */ // 全局总召换20，还有21~36组可以召唤

        CS101_AppLayerParameters alParams = IMasterConnection_getApplicationLayerParameters(connection);

        IMasterConnection_sendACT_CON(connection, asdu, false);

        // 创建遥测点
        createYcPoints(alParams, connection);

        // 创建遥信点
        createYxPoints(alParams, connection);

        IMasterConnection_sendACT_TERM(connection, asdu);
    }
    else
    {
        IMasterConnection_sendACT_CON(connection, asdu, true);
    }

    return true;
}

void createYxPoints(CS101_AppLayerParameters alParams, IMasterConnection connection)
{
    // 创建指定数量的遥信点
    // 第二个数据帧 APDU->ASDU
    int yxIndex = 1; // 遥测点索引
    while (yxIndex <= mYxNum)
    {
        // 创建新的 ASDU
        CS101_ASDU newAsdu = CS101_ASDU_create(alParams, false, CS101_COT_INTERROGATED_BY_STATION,
                                               0, 1, false, false);

        InformationObject io = NULL;

        // 每个 ASDU 最多 40 个遥测点
        for (int i = 0; i < IOA_MERGE_NUM && yxIndex <= mYxNum; i++, yxIndex++)
        {
            if (io == NULL)
            {
                io = (InformationObject)SinglePointInformation_create(NULL, yxIndex, false, IEC60870_QUALITY_GOOD);
                CS101_ASDU_addInformationObject(newAsdu, io);
            }
            else
            {
                InformationObject newIo = (InformationObject)
                    SinglePointInformation_create((MeasuredValueScaled)io, yxIndex, false, IEC60870_QUALITY_GOOD);
                CS101_ASDU_addInformationObject(newAsdu, newIo);
            }
        }

        if (io)
            CS101_ASDU_addInformationObject(newAsdu, io);

        // 发送 ASDU
        IMasterConnection_sendASDU(connection, newAsdu);

        // 清理资源
        if (io)
            InformationObject_destroy(io);
        CS101_ASDU_destroy(newAsdu);
    }
}

// 创建指定数量的遥测点
void createYcPoints(CS101_AppLayerParameters alParams, IMasterConnection connection)
{
    // CS101 规范仅允许 GI 响应中没有时间戳的信息对象
    int ycIndex = 1; // 遥测点索引
    while (ycIndex <= mYcNum)
    {
        // 创建新的 ASDU
        CS101_ASDU newAsdu = CS101_ASDU_create(alParams, false, CS101_COT_INTERROGATED_BY_STATION,
                                               0, 1, false, false);
        InformationObject io = NULL;

        // 每个 ASDU 最多 40 个遥测点
        for (int i = 0; i < IOA_MERGE_NUM && ycIndex <= mYcNum; i++)
        {
            if (io == NULL)
            {
                io = (InformationObject)MeasuredValueScaled_create(NULL, ycIndex++, 0, IEC60870_QUALITY_GOOD);
                CS101_ASDU_addInformationObject(newAsdu, io);
            }
            else
            {
                InformationObject newIo = (InformationObject)
                    MeasuredValueScaled_create((MeasuredValueScaled)io, ycIndex++, 0, IEC60870_QUALITY_GOOD);
                CS101_ASDU_addInformationObject(newAsdu, newIo);
            }
        }

        if (io)
            CS101_ASDU_addInformationObject(newAsdu, io);

        // 发送 ASDU
        IMasterConnection_sendASDU(connection, newAsdu);

        // 清理资源
        if (io)
            InformationObject_destroy(io);
        CS101_ASDU_destroy(newAsdu);
    }
}

// 遥调与遥控处理
static bool
asduHandler(void *parameter, IMasterConnection connection, CS101_ASDU asdu)
{
    if (CS101_ASDU_getTypeID(asdu) == C_SC_NA_1)
    {
        printf("received single command\n");

        if (CS101_ASDU_getCOT(asdu) == CS101_COT_ACTIVATION)
        {
            InformationObject io = CS101_ASDU_getElement(asdu, 0);

            if (io)
            {
                if (InformationObject_getObjectAddress(io) == 5000)
                {
                    SingleCommand sc = (SingleCommand)io;

                    printf("IOA: %i switch to %i\n", InformationObject_getObjectAddress(io),
                           SingleCommand_getState(sc));

                    CS101_ASDU_setCOT(asdu, CS101_COT_ACTIVATION_CON);
                }
                else
                    CS101_ASDU_setCOT(asdu, CS101_COT_UNKNOWN_IOA);

                InformationObject_destroy(io);
            }
            else
            {
                printf("ERROR: message has no valid information object\n");
                return true;
            }
        }
        else
            CS101_ASDU_setCOT(asdu, CS101_COT_UNKNOWN_COT);

        IMasterConnection_sendASDU(connection, asdu);

        return true;
    }

    return false;
}

static bool
connectionRequestHandler(void *parameter, const char *ipAddress)
{
    printf("New connection request from %s\n", ipAddress);

#if 0
    if (strcmp(ipAddress, "127.0.0.1") == 0) {
        printf("Accept connection\n");
        return true;
    }
    else {
        printf("Deny connection\n");
        return false;
    }
#else
    return true;
#endif
}

static void
connectionEventHandler(void *parameter, IMasterConnection con, CS104_PeerConnectionEvent event)
{
    if (event == CS104_CON_EVENT_CONNECTION_OPENED)
    {
        printf("Connection opened (%p)\n", con);
    }
    else if (event == CS104_CON_EVENT_CONNECTION_CLOSED)
    {
        printf("Connection closed (%p)\n", con);
    }
    else if (event == CS104_CON_EVENT_ACTIVATED)
    {
        printf("Connection activated (%p)\n", con);
    }
    else if (event == CS104_CON_EVENT_DEACTIVATED)
    {
        printf("Connection deactivated (%p)\n", con);
    }
}

// 更新遥测数据
int updateIOA(CS104_Slave *slave, CS101_AppLayerParameters *alParams, int type, int16_t value)
{
    int point_num = 1;
    char *type_name = NULL;

    if (type == TYPE_YC)
    {
        point_num = mYcNum;
        type_name = "遥测";
    }
    else if (type == TYPE_YX)
    {
        point_num = mYxNum;
        type_name = "遥信";
    }

    // 定期上送的数据
    for (int i = 1; i <= point_num; i++)
    {
        CS101_ASDU newAsdu = CS101_ASDU_create(alParams, false, CS101_COT_SPONTANEOUS, 0, 1, false, false);

        InformationObject io = NULL;

        if (!io)
        {
            if (type == TYPE_YC)
            {
                io = (InformationObject)MeasuredValueScaled_create(NULL, i, value, IEC60870_QUALITY_GOOD);
            }
            else if (type == TYPE_YX)
            {
                io = (InformationObject)SinglePointInformation_create(NULL, i, value, IEC60870_QUALITY_GOOD);
            }
        }

        CS101_ASDU_addInformationObject(newAsdu, io);
        InformationObject_destroy(io);

        /* Add ASDU to slave event queue */
        // 存储每一帧 frame.c Frame_appendBytes/Frame_getBuffer,由链路层发送(link_layer.c) SerialTransceiverFT12_sendMessage由SerialPort_write
        CS104_Slave_enqueueASDU(slave, newAsdu);
        CS101_ASDU_destroy(newAsdu);
    }

    return value;
}

char *formatTimestamp()
{
    time_t rawtime;
    struct tm *timeinfo;

    // 获取当前时间戳
    time(&rawtime);
    timeinfo = localtime(&rawtime);

    // 为返回的字符串动态分配内存
    char *timestamp = (char *)malloc(20 * sizeof(char)); // 格式: "YYYY-MM-DD HH:MM:SS"
    if (timestamp != NULL)
    {
        // 格式化时间为：年-月-日 时:分:秒
        strftime(timestamp, 20, "%Y-%m-%d %H:%M:%S", timeinfo);
    }

    return timestamp;
}

// 循环调用
int updateIOACycle(CS104_Slave *slave, CS101_AppLayerParameters *alParams)
{
    // 打印指针地址
    // printf("CS104_Slave 指针地址: %p\n", (void *)slave);
    // printf("CS101_AppLayerParameters 指针地址: %p\n", (void *)alParams);

    struct timespec start, end;
    long exec_time, sleep_time;

    // 记录开始时间
    clock_gettime(CLOCK_MONOTONIC, &start);

    // 执行更新
    updateIOA(slave, alParams, TYPE_YC, mYcScaledValue);
    mYcScaledValue++;

    updateIOA(slave, alParams, TYPE_YX, mYxScaledValue);
    mYxScaledValue = !mYxScaledValue;

    // 记录结束时间
    clock_gettime(CLOCK_MONOTONIC, &end);

    // 计算执行时间（毫秒）
    exec_time = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_nsec - start.tv_nsec) / 1000000;
    if (exec_time <= mUpdateSecond * 1000)
    {
        sleep_time = mUpdateSecond * 1000 - exec_time;
    }
    else
    {
        sleep_time = -(mUpdateSecond * 1000 - exec_time);
    }

    // 计算休眠时间
    if (sleep_time > 0)
    {

        printf("%s 更新完成，遥测值: %d, 遥信值: %d, 耗时 %ld 毫秒，休眠 %ld 毫秒后继续。\n",
               formatTimestamp(), mYcScaledValue, mYxScaledValue, exec_time, sleep_time);
        Thread_sleep(sleep_time);
    }
    else
    {
        printf("更新耗时 %ld 毫秒，超过设定间隔 %d 秒，立即进行下一次更新。\n", exec_time, mUpdateSecond);
    }
}

#define BUFFER_SIZE 100
char input_buffer[BUFFER_SIZE];

void *input_thread(void *arg)
{
    while (true)
    {
        if (fgets(input_buffer, BUFFER_SIZE, stdin) != NULL)
        {
            input_buffer[strcspn(input_buffer, "\n")] = '\0'; // 去掉换行符
            if (strcmp(input_buffer, "s") == 0)
            {
                printf("停止自动更新IOA!\n");
                mIsStopAutoUpdateIOA = true;
                break;
            }
        }
        Thread_sleep(1000);
    }
    return NULL;
}

// ./cs104_server --ip=127.0.0.1 --port=502 --ioa_merge --update_second=2 --yx_num=1500 --yc_num=2000
int main(int argc, char **argv)
{
    // 参数解析
    struct option long_options[] = {
        {"ip", required_argument, 0, 'i'},            // 服务端ip
        {"port", required_argument, 0, 'p'},          // 服务监听端口
        {"update_second", required_argument, 0, 'u'}, // 自动变化的更新时间，默认1s, 单位秒
        {"ioa_merge", no_argument, 0, 0},             // ioa是否需要合并上送
        {"yx_num", required_argument, 0, 0},          // 遥信数量
        {"yc_num", required_argument, 0, 0},          // 遥测数量
        {0, 0, 0, 0}                                  // 结束标志
    };

    int opt;
    char *ip = NULL;
    int port = NULL;
    int ioa_merge = 0;
    int update_second = 1;
    int yx_num = 0;
    int yc_num = 0;

    int option_index = 0;
    while ((opt = getopt_long(argc, argv, "i:p:u:", long_options, &option_index)) != -1)
    {
        switch (opt)
        {
        case 'i':
            ip = optarg;
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 'u':
            update_second = atoi(optarg);
            break;
        case 0:
            if (strcmp(long_options[option_index].name, "ioa_merge") == 0)
            {
                ioa_merge = 1;
            }
            else if (strcmp(long_options[option_index].name, "yx_num") == 0)
            {
                yx_num = atoi(optarg);
            }
            else if (strcmp(long_options[option_index].name, "yc_num") == 0)
            {
                yc_num = atoi(optarg);
            }
            break;
        default:
            fprintf(stderr, "Usage: %s --ip=<address> --port=<port> [--ioa_merge] [--yx_num=N] [--yc_num=N]\n", argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    if (yx_num <= 0)
    {
        yx_num = YX_NUM_DEFAULT;
    }

    if (yc_num <= 0)
    {
        yc_num = YC_NUM_DEFAULT;
    }

    if (update_second <= 0)
    {
        update_second = UPDATE_GAP_DEFAULT;
    }

    mYxNum = yx_num;
    mYcNum = yc_num;
    mIOAMerge = ioa_merge;
    mUpdateSecond = update_second;

    printf("IP 地址: %s, 端口是: %d, IOA是否合并: %d, 自动更新频率: %d, 遥信数量: %d, 遥测数量: %d\n",
           ip, port, ioa_merge, update_second, yx_num, yc_num);

    /* Add Ctrl-C handler */
    signal(SIGINT, sigint_handler);

    /* create a new slave/server instance with default connection parameters and
     * default message queue size */
    // 队列大小在冗余模式时会起作用 SINGLE_REDUNDANCY_GROUP
    CS104_Slave slave = CS104_Slave_create((mYcNum + mYxNum) / 10, (mYcNum + mYxNum) / 10);

    CS104_Slave_setLocalAddress(slave, "0.0.0.0");

    /* Set mode to a single redundancy group
     * NOTE: library has to be compiled with CONFIG_CS104_SUPPORT_SERVER_MODE_SINGLE_REDUNDANCY_GROUP enabled (=1)
     */
    // 单冗余组模式
    CS104_Slave_setServerMode(slave, CS104_MODE_SINGLE_REDUNDANCY_GROUP);

    /* get the connection parameters - we need them to create correct ASDUs -
     * you can also modify the parameters here when default parameters are not to be used */
    CS101_AppLayerParameters alParams = CS104_Slave_getAppLayerParameters(slave);
    printf("CS101_AppLayerParameters: sizeOfTypeId=%d, sizeOfVSQ=%d, sizeOfCOT=%d, "
           "originatorAddress=%d, sizeOfCA=%d, sizeOfIOA=%d, maxSizeOfASDU=%d\n",
           alParams->sizeOfTypeId, alParams->sizeOfVSQ, alParams->sizeOfCOT,
           alParams->originatorAddress, alParams->sizeOfCA, alParams->sizeOfIOA, alParams->maxSizeOfASDU);

    /* when you have to tweak the APCI parameters (t0-t3, k, w) you can access them here */
    CS104_APCIParameters apciParams = CS104_Slave_getConnectionParameters(slave);

    printf("APCI parameters:\n");
    printf("  t0: %i,  t1: %i,  t2: %i, t3: %i, k: %i, w: %i\n",
           apciParams->t0, apciParams->t1, apciParams->t2, apciParams->t3, apciParams->k, apciParams->w);

    /* set the callback handler for the clock synchronization command */
    CS104_Slave_setClockSyncHandler(slave, clockSyncHandler, NULL);

    /* set the callback handler for the interrogation command */
    CS104_Slave_setInterrogationHandler(slave, interrogationHandler, NULL);

    /* set handler for other message types */
    CS104_Slave_setASDUHandler(slave, asduHandler, NULL);

    /* set handler to handle connection requests (optional) */
    CS104_Slave_setConnectionRequestHandler(slave, connectionRequestHandler, NULL);

    /* set handler to track connection events (optional) */
    CS104_Slave_setConnectionEventHandler(slave, connectionEventHandler, NULL);

    /* uncomment to log messages */
    // CS104_Slave_setRawMessageHandler(slave, rawMessageHandler, NULL);

    CS104_Slave_start(slave);

    if (CS104_Slave_isRunning(slave) == false)
    {
        printf("Starting server failed!\n");
        goto exit_program;
    }

    // 遥信点 SinglePointInformation_create
    // 遥测点 MeasuredValueNormalized_create
    // 遥调点 SetpointCommandNormalized_create
    // 遥控点 SingleCommand_create
    bool auto_mode = false; // 是否自动更新
    bool manu_mode = false; // 手动更新

    while (running)
    {
        printf("请输入模式 (m=手动更新, a=自动更新, s=停止自动更新, q=退出): ");
        char input = getchar();

        // 清除输入缓冲区
        while (getchar() != '\n')
            ;

        switch (input)
        {
        case 'm':
            manu_mode = true;
            printf("切换到手动更新模式。\n");
            break;
        case 'a':
            auto_mode = true;
            printf("切换到自动更新模式。\n");
            mIsStopAutoUpdateIOA = false;

            void *args[] = {};
            mUpdateThread = Thread_create(input_thread, args, false);
            Thread_start(mUpdateThread);

            while (!mIsStopAutoUpdateIOA)
            {
                updateIOACycle(slave, alParams);
            }

            if (mUpdateThread != NULL)
            {
                Thread_destroy(mUpdateThread);
                mUpdateThread = NULL;
            }
            auto_mode = false;

            // 更新的线程要和创建连接的线程一致
            // start_auto_update(slave, alParams);
            break;
        case 's':
            auto_mode = false;
            printf("停止更新数据！\n");
            break;
        case 'q':
            running = false;
            if (mUpdateThread != NULL)
            {
                Thread_destroy(mUpdateThread);
            }
            printf("退出整个测试程序。\n");
            break;
        default:
            printf("无效输入，请输入 'm'、'a'、's' 或 'q'。\n");
        }

        manu_mode = false;
        Thread_sleep(100);
    }

    CS104_Slave_stop(slave);

exit_program:
    CS104_Slave_destroy(slave);

    Thread_sleep(500);
}
