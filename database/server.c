#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "strings.h"
#include "ctype.h"
#include "unistd.h"
#include "dirent.h"
#include "errno.h"

#include "netinet/in.h" 

#include "sys/stat.h"
#include "sys/types.h"
#include "sys/epoll.h"

#ifndef __DATA_BUFFER
	#define __DATA_BUFFER 4096
#else
	#error __DATA_BUFFER already defined
#endif

#ifndef __MAX_CONNECTIONS
	#define __MAX_CONNECTIONS 63
#else
	#error __MAX_CONNECTIONS already defined
#endif

#ifndef __SERVER_PORT
	#define __SERVER_PORT 1122 
#else
	#error __SERVER_PORT already defined
#endif

#ifndef __SERVER_TIME_OUT_MSEC
	#define __SERVER_TIME_OUT_MSEC 100 
#else
	#error __SERVER_TIME_OUT_MSEC already defined
#endif

#ifndef __DATABASE_ROOT
	#define __DATABASE_ROOT "databases"
#else
	#error __DATABASE_ROOT already defined
#endif

#ifndef __MAX_ATTRIBUTE_NAME_LENGTH
	#define __MAX_ATTRIBUTE_NAME_LENGTH 64
#else
	#error __MAX_ATTRIBUTE_NAME_LENGTH already defined
#endif

#ifndef __MAX_ATTRIBUTE_ON_TABLE
	#define __MAX_ATTRIBUTE_ON_TABLE 128
#else
	#error __MAX_ATTRIBUTE_ON_TABLE already defined
#endif

#ifndef __STRING_MAX_LENGTH
	#define __STRING_MAX_LENGTH 655356
#else
	#error __STRING_MAX_LENGTH already defined
#endif

typedef enum {
	INT = 1, 
	LONG = 2, 
	DECIMAL = 3, 
	STRING = 4, 
	TIME = 5, 
	DATE = 6, 
	DATETIME = 7
} DataType;

typedef enum {
	EMPTY = 0,
	FILLED = 1
} BLOCKFLAG;

typedef struct {
	char attributeName[__MAX_ATTRIBUTE_NAME_LENGTH];
	DataType type;
	int size;
} Attribute;

typedef struct {
	BLOCKFLAG flag;
	Attribute attribute;
} AttributeBlock;

typedef struct {
	BLOCKFLAG flag;
	int size;
	void *data;
} RecordBlock;

typedef struct {
	RecordBlock *record;
	int size;
	int capacity;
} RecordBlockVector;

typedef struct {
	void *block;
	int size;
	int capacity;
} DynamicBlock;

typedef struct ParsedStringQueue {
	struct ParsedStringQueue *next;
	char *parsedString;
} ParsedStringQueue;

typedef struct {
	int id;
	int openningDatabase;
	char databaseName[64];
} AccountData;

void initRecordBlock(RecordBlock *recordBlock, int size) 
{
	recordBlock->data = malloc(sizeof(char) * size);
	memset(recordBlock->data, 0, sizeof(char) * size);
	recordBlock->size = sizeof(char) * size;
	recordBlock->flag = FILLED;
}

size_t sizeOfRecordBlock(RecordBlock *recordBlock)
{
	return recordBlock->size + sizeof(recordBlock->size) + sizeof(recordBlock->flag);
}

void delRecordBlock(RecordBlock *recordBlock)
{
	free(recordBlock->data);
}

size_t fwriteRecordBlock(RecordBlock *recordBlock, FILE *stream)
{
	return 
		fwrite(&(recordBlock->flag), sizeof(recordBlock->flag), 1, stream) +
		fwrite(&(recordBlock->size), sizeof(recordBlock->size), 1, stream) +
		fwrite(recordBlock->data, recordBlock->size, 1, stream);
}

size_t freadRecordBlock(RecordBlock *recordBlock, FILE *stream)
{
	if (
		fread(&(recordBlock->flag), sizeof(recordBlock->flag), 1, stream) +
		fread(&(recordBlock->size), sizeof(recordBlock->size), 1, stream) +
		fread(recordBlock->data, recordBlock->size, 1, stream) == 3
	)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

void initDynamicBlock(DynamicBlock *dBlock)
{
	dBlock->block = malloc(sizeof(char) * 2);
	dBlock->size = 0;
	dBlock->capacity = 2;
}

void delDynamicBlock(DynamicBlock *dBlock)
{
	free(dBlock->block);
}

void concatDynamicBlock(DynamicBlock *dBlock, const void *block, size_t size)
{
	while (dBlock->size + size > dBlock->capacity)
	{
		dBlock->capacity *= 2;
		void *newBlock = malloc(sizeof(char) * dBlock->capacity);
		memset(newBlock, 0, sizeof(char) * dBlock->capacity);
		memcpy(newBlock, dBlock->block, sizeof(char) * dBlock->size);
		
		free(dBlock->block);
		dBlock->block = newBlock;
	}
	
	memcpy(dBlock->block + dBlock->size, block, size);
	dBlock->size += size;
}

void initRecordBlockVector(RecordBlockVector *vector)
{
	vector->record = (RecordBlock *)malloc(sizeof(RecordBlock) * 2);
	vector->capacity = 2;
	vector->size = 0;
}

RecordBlock* getRecordBlockVector(RecordBlockVector *vector)
{
	return vector->record;
}

int sizeRecordBlockVector(RecordBlockVector *vector)
{
	return vector->size;
}

void delRecordBlockVector(RecordBlockVector *vector)
{
	for (int i = 0; i < vector->size; i++)
	{
		delRecordBlock(&(vector->record[i]));
	}
	free(vector->record);
}

void appendRecordBlockVector(RecordBlockVector *vector, RecordBlock *data)
{	
	if (vector->size >= vector->capacity)
	{
		vector->capacity *= 2;
		RecordBlock *newRecordBlock = (RecordBlock *)malloc(sizeof(RecordBlock) * vector->capacity);
		
		memcpy(newRecordBlock, vector->record, sizeof(RecordBlock) * vector->size);

		free(vector->record);
		vector->record = newRecordBlock;
	}
	
	vector->record[vector->size] = *data;
	vector->size += 1;
}

ParsedStringQueue* parseStringSQLScript(char str[])
{
	int scriptLength = strlen(str);
	int index = 0;
	
	ParsedStringQueue *root = NULL;
	ParsedStringQueue *walker = root;
	ParsedStringQueue *newString = NULL;
	int parentheses = 0;
	
	for (int i = 0; i < scriptLength; i++)
	{
		if (str[i] != ' ' && str[i] != ';')
		{
			for (int j = i + 1; j < scriptLength; j++)
			{
				if (str[j] == '(')
				{
					parentheses++;
				}
				else if (str[j] == ')')
				{
					parentheses--;
				}
				if ((str[i] == '(' && str[j] == ')' && parentheses == 0) || (str[i] != '(' && (str[j] == ' ' || str[j] == ';')))
				{
					newString = (ParsedStringQueue *)malloc(sizeof(ParsedStringQueue));
					newString->next = NULL;
					if (str[i] == '(')
					{
						i++;
					}
					
					newString->parsedString = malloc(sizeof(char) * (j - i + 1));
					memset(newString->parsedString, 0, sizeof(char) * (j - i + 1));
					strncpy(newString->parsedString, str + i, (j - i));
					
					if (root == NULL)
					{
						root = newString;
						walker = newString;
					}
					else
					{
						walker->next = newString;
						walker = newString;
					}
					
					i = j + 1;
				}
			}
		}
	}
	
	return root;
}

void popParsedStringQueue(ParsedStringQueue **queue)
{
	ParsedStringQueue *next = (*queue)->next;
	free((*queue)->parsedString);
	free(*queue);
	*queue = next;
}

int isStringAlphaNumeric(char str[])
{
	int length = strlen(str);
	for (int i = 0; i < length; i++)
	{
		if (isalnum(str[i]) == 0)
		{
			return 0;
		}
	}
	return 1;
}

int createTCPServerSocket()
{
    struct sockaddr_in socketAddress;
    int socketFileDescriptor = -1;

	socketFileDescriptor = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (socketFileDescriptor == -1) {
        fprintf(stderr, "Error: [%s]\n", strerror(errno));
        return -1;
    }

    socketAddress.sin_family = AF_INET;         
    socketAddress.sin_port = htons(__SERVER_PORT);     
    socketAddress.sin_addr.s_addr = INADDR_ANY; 

    if (bind(socketFileDescriptor, (struct sockaddr *)&socketAddress, sizeof(struct sockaddr_in)) != 0) {
        fprintf(stderr, "Error: [%s]\n", strerror(errno));
        close(socketFileDescriptor);
        return -1;
    }

    if (listen(socketFileDescriptor, 64) != 0) {
        fprintf(stderr, "Error: [%s]\n", strerror(errno));
        close(socketFileDescriptor);
        return -1;
    }

    return socketFileDescriptor;
}

void setupEpollConnection(int epollFileDescriptor, int newFileDescriptor, struct epoll_event * epollEvent) 
{
    epollEvent->events = EPOLLIN;
    epollEvent->data.fd = newFileDescriptor;

    epoll_ctl(epollFileDescriptor, EPOLL_CTL_ADD, newFileDescriptor, epollEvent);
}

int qsortFunctionForAttribute(const void *a, const void *b)
{
	return strcmp( ((Attribute *)a)->attributeName, ((Attribute *)b)->attributeName );
}

int noDuplicateAttribute(int attributeAmount, Attribute attribute[])
{
	Attribute temp[attributeAmount];
	memcpy(temp, attribute, sizeof(Attribute) * attributeAmount);
	qsort(temp, attributeAmount, sizeof(Attribute), qsortFunctionForAttribute);
	for (int i = 0; i < attributeAmount - 1; i++)
	{
		if (strcmp(temp[i].attributeName, temp[i+1].attributeName) == 0)
		{
			return 0;
		}
	}
	return 1;
}

int createTable(char database[], char table[], int attributeAmount, Attribute attribute[])
{
	char filePath[1024];
	sprintf(filePath, "%s/%s/%s", __DATABASE_ROOT, database, table);
	FILE *tableFile = fopen(filePath, "r");
	
	if (tableFile == NULL && attributeAmount <= __MAX_ATTRIBUTE_ON_TABLE && noDuplicateAttribute(attributeAmount, attribute))
	{
		tableFile = fopen(filePath, "w");
		
		int tableData[3];
		memset(tableData, 0, sizeof(int) * 3);
		tableData[0] = attributeAmount;
		
		size_t attributeBlockSize = attributeAmount * sizeof(AttributeBlock);
		void *attributeBlockArray = malloc(attributeBlockSize);
		memset(attributeBlockArray, 0, attributeBlockSize);
		
		AttributeBlock attributeBlock[attributeAmount];
		for (int i = 0; i < attributeAmount; i++)
		{
			attributeBlock[i].flag = FILLED;
			attributeBlock[i].attribute = attribute[i];
			tableData[2] += attribute[i].size;
		}
		memcpy(attributeBlockArray, attributeBlock, sizeof(AttributeBlock) * attributeAmount);
		
		fwrite(tableData, sizeof(int), 3, tableFile);
		fwrite(attributeBlockArray, sizeof(AttributeBlock), attributeAmount, tableFile);
		
		fclose(tableFile);
		free(attributeBlockArray);
		
		return 1;
	}
	return 0;
}

int createDatabase(char databaseName[])
{
	char filePath[1024];
	sprintf(filePath, "%s/%s", __DATABASE_ROOT, databaseName); 
	DIR *databaseDirectory = opendir(filePath);
	if (databaseDirectory == NULL)
	{
		mkdir(filePath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
		return 1;
	}
	
	return 0;
}

void determineAttributeDataType(Attribute attribute[], int *totalAttribute, char attributeType[])
{
	if (strcasecmp(attributeType, "INT") == 0)
	{
		attribute[*totalAttribute].type = INT;
		attribute[*totalAttribute].size = sizeof(int);
	}
	else if (strcasecmp(attributeType, "LONG") == 0)
	{
		attribute[*totalAttribute].type = LONG;
		attribute[*totalAttribute].size = sizeof(long long int);
	}
	else if (strcasecmp(attributeType, "DECIMAL") == 0)
	{
		attribute[*totalAttribute].type = DECIMAL;
		attribute[*totalAttribute].size = sizeof(double);
	}
	else if (strncasecmp(attributeType, "STRING", 6) == 0)
	{
		attribute[*totalAttribute].type = STRING;
		
		int length = strlen(attributeType);
		if (length > 6 && attributeType[6] == '(')
		{
			int size;
			sscanf(attributeType + 7, "%d", &size);
			if (size > __STRING_MAX_LENGTH) 
			{
				attribute[*totalAttribute].size = __STRING_MAX_LENGTH;
			}
			else
			{
				attribute[*totalAttribute].size = size;
			}
		}
		else
		{
			attribute[*totalAttribute].size = __STRING_MAX_LENGTH;
		}
	}
	else if (strcasecmp(attributeType, "TIME") == 0)
	{
		
		attribute[*totalAttribute].type = TIME;
		attribute[*totalAttribute].size = sizeof(char) * 5;
	}
	else if (strcasecmp(attributeType, "DATE") == 0)
	{
		
		attribute[*totalAttribute].type = DATE;
		attribute[*totalAttribute].size = sizeof(char) * 10;
	}
	else if (strcasecmp(attributeType, "DATETIME") == 0)
	{
		
		attribute[*totalAttribute].type = DATETIME;
		attribute[*totalAttribute].size = sizeof(char) * 16;
	}
	*totalAttribute += 1;
}
	
int parseAttribute(Attribute attribute[], int *totalAttribute, char str[])
{
	int strLength = strlen(str);
	*totalAttribute = 0;
	
	int offset = 0;
	
	char attributeType[32];
	int stateInserted = 0;
	
	for (int i = 0; i < strLength; i++)
	{
		if (str[i] == ' ' || str[i] == ',')
		{
			if (stateInserted == 0)
			{
				memset(&(attribute[*totalAttribute]), 0, sizeof(Attribute));
				strncpy(attribute[*totalAttribute].attributeName, str + offset, i - offset);
				stateInserted = 1;
				
				if (isStringAlphaNumeric(attribute[*totalAttribute].attributeName) == 0)
				{
					return 0;
				}
			}
			else
			{
				memset(attributeType, 0, sizeof(attributeType));
				strncpy(attributeType, str + offset, i - offset);
				determineAttributeDataType(attribute, totalAttribute, attributeType);
				stateInserted = 0;
			}
			offset = i + 1;
			
			while(offset < strLength && str[offset] == ' ')
			{
				offset++;
			}
			i = offset;
		}
		else if (i == strLength - 1)
		{
			if (stateInserted == 0)
			{
				return 0;
			}
			strncpy(attributeType, str + offset, i - offset + 1);
			determineAttributeDataType(attribute, totalAttribute, attributeType);
		}
	}
	
	return 1; 
}

int createDatabaseRoot()
{ 
	DIR *databaseDirectory = opendir(__DATABASE_ROOT);
	if (databaseDirectory == NULL)
	{
		mkdir(__DATABASE_ROOT, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
		createDatabase("admin");
		
		Attribute attribute[__MAX_ATTRIBUTE_ON_TABLE];
		int attributeAmount = 0;
		char tableCreateAttributeString[256];
		sprintf(tableCreateAttributeString, "id INT, username STRING(64), password STRING(64)");
		if (parseAttribute(attribute, &attributeAmount, tableCreateAttributeString) == 1)
		{
			createTable("admin", "account", attributeAmount, attribute);
		}
		
		
		attributeAmount = 0;
		sprintf(tableCreateAttributeString, "id INT, name STRING(64)");
		if (parseAttribute(attribute, &attributeAmount, tableCreateAttributeString) == 1)
		{
			createTable("admin", "database", attributeAmount, attribute);
		}
		
		attributeAmount = 0;
		sprintf(tableCreateAttributeString, "accountID INT, databaseID INT");
		if (parseAttribute(attribute, &attributeAmount, tableCreateAttributeString) == 1)
		{
			createTable("admin", "database_permission", attributeAmount, attribute);
		}
		
		return 1;
	}
	return 0;
}

int insertIntoDatabaseTable(char database[], char table[], RecordBlock *newRecordBlock)
{
	char filePath[1024];
	sprintf(filePath, "%s/%s/%s", __DATABASE_ROOT, database, table);
	FILE *tableFile = fopen(filePath, "r+");
	
	if (tableFile == NULL)
	{
		return 0;
	}
	
	int tableData[3];
	fread(tableData, sizeof(tableData[0]), 3, tableFile);
	
	fseek(tableFile, sizeof(AttributeBlock) * tableData[0], SEEK_CUR);
	
	RecordBlock reader;
	initRecordBlock(&reader, tableData[2]);
	size_t recordBlockMemorySize = sizeOfRecordBlock(newRecordBlock);
	
	while(freadRecordBlock(&reader, tableFile) == 1)
	{
		if (reader.flag == EMPTY) {
			fseek(tableFile, -recordBlockMemorySize, SEEK_CUR); 
			break;
		}
	}
	delRecordBlock(&reader);
	
	fwriteRecordBlock(newRecordBlock, tableFile);
	
	fseek(tableFile, 0, SEEK_SET);
	tableData[1]++;
	fwrite(tableData, sizeof(int), 3, tableFile);
	
	fclose(tableFile);

	return 1;
}

int readTableAttribute(char database[], char table[], int *totalAttribute, Attribute attribute[], int *recordBlockSize)
{
	char filePath[1024];
	sprintf(filePath, "%s/%s/%s", __DATABASE_ROOT, database, table);
	FILE *tableFile = fopen(filePath, "r");
	
	*totalAttribute = 0;
	
	if (recordBlockSize != NULL)
	{
		*recordBlockSize = 0;
	}
	
	if (tableFile != NULL)
	{
		int tableData[3];
		fread(tableData, sizeof(int), 3, tableFile);
		
		if (recordBlockSize != NULL)
		{
			*recordBlockSize = tableData[2];
		}
		
		AttributeBlock attributeBlock[tableData[0]];
		fread(attributeBlock, sizeof(attributeBlock[0]), tableData[0], tableFile);
		
		*totalAttribute = tableData[0];
		
		for (int i = 0; i < tableData[0]; i++)
		{
			memcpy(&attribute[i], &(attributeBlock[i].attribute), sizeof(Attribute));
		}
		
		fclose(tableFile);
		
		return 1;
	}
	return 0;
}

int insertIntoDatabaseScript(ParsedStringQueue **queue, AccountData *clientAccount)
{
	if (clientAccount->openningDatabase == 1)
	{
		char tableName[64];
		strcpy(tableName, (*queue)->parsedString);
		
		popParsedStringQueue(queue);
		
		char filePath[1024];
		sprintf(filePath, "%s/%s/%s", __DATABASE_ROOT, clientAccount->databaseName, tableName);
		FILE *tableFile = fopen(filePath, "r");
		
		if (tableFile != NULL && *queue != NULL)
		{
			Attribute attribute[__MAX_ATTRIBUTE_ON_TABLE];
			int totalAttribute = 0;
			int recordBlockSize = 0;
			if (readTableAttribute(clientAccount->databaseName, tableName, &totalAttribute, attribute, &recordBlockSize) == 1)
			{
				RecordBlock recordBlockForNewData;
				initRecordBlock(&recordBlockForNewData, recordBlockSize);
				int recordBlockDataOffset = 0;
				
				int result = 1;
				
				int strLength = strlen((*queue)->parsedString);
				char *stringRecordData = (*queue)->parsedString;
				int stringRecordDataOffset = 0;
				int flagSingleQuote = 0;
				
				for (int i = 0; i < strLength; i++)
				{
					if (stringRecordData[i] == '\'')
					{
						if (flagSingleQuote == 0)
						{
							flagSingleQuote = 1;
						}
						else
						{
							flagSingleQuote = 0;
						}
						stringRecordData[i] = '\0';
					}
					else if ((stringRecordData[i] == ' ' || stringRecordData[i] == ',') && flagSingleQuote == 0)
					{
						stringRecordData[i] = '\0';
					}
				}
				
				while(stringRecordDataOffset < strLength && stringRecordData[stringRecordDataOffset] == '\0')
				{
					stringRecordDataOffset++;
				}
				
				for (int i = 0; i < totalAttribute && result == 1 && stringRecordDataOffset < strLength; i++)
				{
					if (
						attribute[i].type == STRING || attribute[i].type == TIME || 
						attribute[i].type == DATE || attribute[i].type == DATETIME
					)
					{	
						int stringLength = strlen(stringRecordData + stringRecordDataOffset);
						memcpy(
							recordBlockForNewData.data + recordBlockDataOffset, stringRecordData + stringRecordDataOffset, 
							attribute[i].size < stringLength ? attribute[i].size : stringLength
						);
					}
					else 
					{
						if (attribute[i].type == INT)
						{
							int data = 0;
							result = sscanf(stringRecordData + stringRecordDataOffset, "%d", &data);
							memcpy(recordBlockForNewData.data + recordBlockDataOffset, &data, sizeof(data));
						}
						else if (attribute[i].type == LONG)
						{
							long long int data = 0;
							result = sscanf(stringRecordData + stringRecordDataOffset, "%lld", &data);
							memcpy(recordBlockForNewData.data + recordBlockDataOffset, &data, sizeof(data));
						}
						else if (attribute[i].type == DECIMAL)
						{
							double data = 0;
							result = sscanf(stringRecordData + stringRecordDataOffset, "%lf", &data);
							memcpy(recordBlockForNewData.data + recordBlockDataOffset, &data, sizeof(data));
						}
					}
					
					while(stringRecordDataOffset < strLength && stringRecordData[stringRecordDataOffset] != '\0')
					{
						stringRecordDataOffset++;
					}
					while(stringRecordDataOffset < strLength && stringRecordData[stringRecordDataOffset] == '\0')
					{
						stringRecordDataOffset++;
					}
					recordBlockDataOffset += attribute[i].size;
				}
				
				if (result == 1)
				{
					insertIntoDatabaseTable(clientAccount->databaseName, tableName, &recordBlockForNewData);
				}
				
				delRecordBlock(&recordBlockForNewData);
				return result;
			}
			
		}
	}
	return 0;
}

int selectReadTable(
	char database[], char table[], RecordBlockVector *records, Attribute attribute[], int *attributeTotal, 
	int *recordBlockSize, char *whereAttr, void *whereValue
)
{
	char filePath[1024];
	sprintf(filePath, "%s/%s/%s", __DATABASE_ROOT, database, table);
	FILE *tableFile = fopen(filePath, "r");
	
	if (tableFile == NULL)
	{
		return 0;
	}
	
	*attributeTotal = 0;
	*recordBlockSize = 0;
	
	int tableData[3];
	fread(tableData, sizeof(tableData[0]), 3, tableFile);
	
	*attributeTotal = 0;
	AttributeBlock attributeBlock[tableData[0]];
	AttributeBlock reader;
	for (int i = 0; i < tableData[0]; i++)
	{
		fread(&reader, sizeof(reader), 1, tableFile);
		if (reader.flag == FILLED)
		{
			memcpy(&(attributeBlock[*attributeTotal]), &reader, sizeof(reader));
			memcpy(&(attribute[*attributeTotal]), &(attributeBlock[*attributeTotal].attribute), sizeof(Attribute));
			*attributeTotal += 1;
		}
	}
	
	int whereIndex = -1;
	if (whereAttr != NULL)
	{
		int error = 1;
		for(int i = 0; i < *attributeTotal && error != 0; i++)
		{
			if (strcmp(whereAttr, attribute[i].attributeName) == 0)
			{
				whereIndex = i;
				error = 0;
			}
		}
		if (error != 0)
		{
			return 0;
		}
	}
	
	int whereOffsetBytePosition = 0;
	*recordBlockSize = tableData[2];
	for (int i = 0; i < *attributeTotal; i++)
	{
		if (i < whereIndex)
		{
			whereOffsetBytePosition += attribute[i].size;
		}
	}
	
	fseek(tableFile, sizeof(tableData) + tableData[0] * sizeof(AttributeBlock), SEEK_SET);
	
	RecordBlock recordBlockData;
	int recordBlockSizeMemory = *recordBlockSize + sizeof(BLOCKFLAG) + sizeof(int);
	char recordByte[recordBlockSizeMemory];
	size_t offsetDataByte = sizeof(BLOCKFLAG) + sizeof(int);
	whereOffsetBytePosition += offsetDataByte;
	
	while(fread(recordByte, sizeof(char), recordBlockSizeMemory, tableFile) == recordBlockSizeMemory)
	{
		if (whereAttr != NULL && memcmp(recordByte + whereOffsetBytePosition, whereValue, attribute[whereIndex].size) == 0)
		{
			initRecordBlock(&recordBlockData, *recordBlockSize);
			memcpy(&recordBlockData, recordByte, offsetDataByte);
			memcpy(recordBlockData.data, recordByte + offsetDataByte, *recordBlockSize);
			appendRecordBlockVector(records, &recordBlockData);
		}
		else if (whereAttr == NULL)
		{
			initRecordBlock(&recordBlockData, *recordBlockSize);
			memcpy(&recordBlockData, recordByte, offsetDataByte);
			memcpy(recordBlockData.data, recordByte + offsetDataByte, *recordBlockSize);
			appendRecordBlockVector(records, &recordBlockData);
		}
	}
	
	fclose(tableFile);
	return 1;
}

void readTableDataBlock(char database[], char table[], int tableData[])
{
	char filePath[1024];
	sprintf(filePath, "%s/%s/%s", __DATABASE_ROOT, database, table);
	FILE *tableFile = fopen(filePath, "r");
	
	if (tableFile == NULL)
	{
		return;
	}
	
	fread(tableData, sizeof(int), 3, tableFile);
	fclose(tableFile);
}

int createNewAccount(ParsedStringQueue **queue)
{
	char username[64];
	memset(username, 0, sizeof(username));
	strcpy(username, (*queue)->parsedString);
	
	RecordBlockVector recordBlockVector;
	initRecordBlockVector(&recordBlockVector);
	
	Attribute attribute[__MAX_ATTRIBUTE_ON_TABLE];
	int attributeTotal = 0;
	int recordBlockSize = 0;
	
	char usernameString[64];
	memset(usernameString, 0, 64);
	strcpy(usernameString, "username");
	
	selectReadTable("admin", "account", &recordBlockVector, attribute, &attributeTotal, &recordBlockSize, usernameString, username);
	
	if (recordBlockVector.size != 0)
	{
		delRecordBlockVector(&recordBlockVector);
		return 0;
	}
	delRecordBlockVector(&recordBlockVector);
	
	popParsedStringQueue(queue);
	
	if (*queue == NULL || strcasecmp((*queue)->parsedString, "IDENTIFIED" ) != 0)
	{
		return 0;
	}
	
	popParsedStringQueue(queue);
	
	if (*queue == NULL || strcasecmp((*queue)->parsedString, "BY" ) != 0)
	{
		return 0;
	}
	
	popParsedStringQueue(queue);
	
	if (*queue == NULL)
	{
		return 0;
	}
	
	char password[64];
	memset(password, 0, sizeof(password));
	strcpy(password, (*queue)->parsedString);
	
	RecordBlock recordBlockNewAccount;
	initRecordBlock(&recordBlockNewAccount, recordBlockSize);
	
	int tableData[3];
	readTableDataBlock("admin", "account", tableData);
	tableData[1] += 1;
	
	memcpy(recordBlockNewAccount.data, &tableData[1], sizeof(int));
	memcpy(recordBlockNewAccount.data + sizeof(int), username, sizeof(username)); 
	memcpy(recordBlockNewAccount.data + sizeof(int) + sizeof(username), password, sizeof(password));
	
	insertIntoDatabaseTable("admin", "account", &recordBlockNewAccount);
	
	delRecordBlock(&recordBlockNewAccount);
	return 1;
}

int createNewDatabase(ParsedStringQueue **queue, int userID)
{
	if (*queue != NULL)
	{
		char databaseName[64];
		memset(databaseName, 0, sizeof(databaseName));
		strcpy(databaseName, (*queue)->parsedString);
		
		if (isStringAlphaNumeric(databaseName) == 1)
		{
			RecordBlockVector recordBlockVector;
			initRecordBlockVector(&recordBlockVector);
			Attribute attribute[__MAX_ATTRIBUTE_ON_TABLE];
			int attributeTotal = 0;
			int recordBlockSize = 0;
			
			selectReadTable(
				"admin", "database", &recordBlockVector, attribute, 
				&attributeTotal, &recordBlockSize, "name", databaseName
			);
			
			if (recordBlockVector.size == 0)
			{
				int tableData[3];
				readTableDataBlock("admin", "database", tableData);
				int databaseID = tableData[1] + 1;
				
				RecordBlock recordBlockForNewDatabase;
				initRecordBlock(&recordBlockForNewDatabase, recordBlockSize);
				memcpy(recordBlockForNewDatabase.data, &databaseID, sizeof(int));
				memcpy(recordBlockForNewDatabase.data + sizeof(int), databaseName, sizeof(databaseName));
				insertIntoDatabaseTable("admin", "database", &recordBlockForNewDatabase);
				
				delRecordBlock(&recordBlockForNewDatabase);
				
				readTableDataBlock("admin", "database_permission", tableData);
				initRecordBlock(&recordBlockForNewDatabase, tableData[2]);
				memcpy(recordBlockForNewDatabase.data, &userID, sizeof(userID));
				memcpy(recordBlockForNewDatabase.data + sizeof(userID), &databaseID, sizeof(databaseID));
				insertIntoDatabaseTable("admin", "database_permission", &recordBlockForNewDatabase);
				
				createDatabase(databaseName);
				
				delRecordBlock(&recordBlockForNewDatabase);
				return 1;
			}
			
			delRecordBlockVector(&recordBlockVector);		
		}
	}
	
	return 0;
}

int grantPermissionUserOnDatabase(ParsedStringQueue **queue)
{
	if (*queue == NULL)
	{
		return 0;
	}
	
	char databaseName[64];
	memset(databaseName, 0, sizeof(databaseName));
	strcpy(databaseName, (*queue)->parsedString);
	
	RecordBlockVector recordBlockVector;
	initRecordBlockVector(&recordBlockVector);
	Attribute attribute[__MAX_ATTRIBUTE_ON_TABLE];
	int attributeTotal = 0;
	int recordBlockSize = 0;
		
	selectReadTable("admin", "database", &recordBlockVector, attribute, &attributeTotal, &recordBlockSize, "name", databaseName);
	
	popParsedStringQueue(queue);
	if (*queue == NULL || strcasecmp((*queue)->parsedString, "INTO") != 0 || recordBlockVector.size != 1)
	{
		return 0;
	}
	popParsedStringQueue(queue);
	
	int databaseID = -1;
	memcpy(&databaseID, recordBlockVector.record[0].data, sizeof(databaseID));
	delRecordBlockVector(&recordBlockVector);
	
	if (*queue == NULL)
	{
		return 0;
	}
	
	char username[64];
	memset(username, 0, sizeof(username));
	strcpy(username, (*queue)->parsedString);
	
	initRecordBlockVector(&recordBlockVector);
	attributeTotal = 0;
	recordBlockSize = 0;
	selectReadTable("admin", "account", &recordBlockVector, attribute, &attributeTotal, &recordBlockSize, "username", username);
	
	if (recordBlockVector.size == 1)
	{
		int accountID = 0;
		memcpy(&accountID, recordBlockVector.record[0].data, sizeof(accountID));
		
		RecordBlock newRecordPermission;
		int recordData[2] = {accountID, databaseID};
		initRecordBlock(&newRecordPermission, sizeof(recordData));
		memcpy(newRecordPermission.data, recordData, sizeof(recordData));
		
		insertIntoDatabaseTable("admin", "database_permission", &newRecordPermission);
		
		delRecordBlock(&newRecordPermission);
	}
	else
	{
		delRecordBlockVector(&recordBlockVector);
		return 0;
	}
	
	delRecordBlockVector(&recordBlockVector);
	
	return 1;
}

int useDatabase(ParsedStringQueue **queue, AccountData *clientAccount)
{
	clientAccount->openningDatabase = 0;

	if (*queue != NULL) 
	{
		char databaseName[64];
		memset(databaseName, 0, sizeof(databaseName));
		strcpy(databaseName, (*queue)->parsedString);
		
		Attribute attribute[__MAX_ATTRIBUTE_ON_TABLE];
		RecordBlockVector recordBlockVector;
		initRecordBlockVector(&recordBlockVector);
		int attributeTotal = 0;
		int recordBlockSize = 0;
		selectReadTable("admin", "database", &recordBlockVector, attribute, &attributeTotal, &recordBlockSize, "name", databaseName);

		if (recordBlockVector.size == 1)
		{
			if (clientAccount->id == 0)
			{
				clientAccount->openningDatabase = 1;
				memcpy(clientAccount->databaseName, databaseName, sizeof(databaseName));
			}
			else
			{
				int databaseID = 0;
				memcpy(&databaseID, recordBlockVector.record[0].data, sizeof(databaseID));
				delRecordBlockVector(&recordBlockVector);
				
				initRecordBlockVector(&recordBlockVector);
				attributeTotal = 0;
				recordBlockSize = 0;
				selectReadTable(
					"admin", "database_permission", &recordBlockVector, 
					attribute, &attributeTotal, &recordBlockSize, "accountID", &clientAccount->id
				);
				
				int pointerOffsetForDatabaseID = 0;
				for (int i = 0; i < attributeTotal; i++)
				{
					if (strcmp(attribute[i].attributeName, "databaseID") == 0)
					{
						break;
					}
					pointerOffsetForDatabaseID += attribute[i].size;
				}
				for (int i = 0; i < recordBlockVector.size; i++)
				{
					if (memcmp(recordBlockVector.record[i].data + pointerOffsetForDatabaseID, &databaseID, attribute[1].size) == 0)
					{
						clientAccount->openningDatabase = 1;
						memcpy(clientAccount->databaseName, databaseName, sizeof(databaseName));
						break;
					}
				}
			}
		}

		delRecordBlockVector(&recordBlockVector);
	}
	
	if (clientAccount->openningDatabase == 1)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

int createTableByUser(ParsedStringQueue **queue, AccountData *clientAccountData)
{
	if (*queue == NULL || clientAccountData->openningDatabase != 1)
	{
		return 0;
	}
	char tableName[64];
	strcpy(tableName, (*queue)->parsedString);
	
	if (isStringAlphaNumeric(tableName) == 1)
	{
		popParsedStringQueue(queue);
		
		Attribute attribute[__MAX_ATTRIBUTE_ON_TABLE];
		int attributeAmount = 0;
		if (parseAttribute(attribute, &attributeAmount, (*queue)->parsedString) == 1)
		{
			if (createTable(clientAccountData->databaseName, tableName, attributeAmount, attribute) == 1)
			{
				return 1;
			}
		}
	}
	return 0;
}

int deleteFromDatabaseTable(char database[], char table[], char *whereAttr, void *whereValue)
{
	char filePath[1024];
	sprintf(filePath, "%s/%s/%s", __DATABASE_ROOT, database, table);
	FILE *tableFile = fopen(filePath, "r+");
	
	if (tableFile == NULL)
	{
		return -1;
	}
	
	int tableData[3];
	fread(tableData, sizeof(tableData[0]), 3, tableFile);
	
	AttributeBlock attributesBlock[tableData[0]];
	fread(attributesBlock, sizeof(attributesBlock[0]), tableData[0], tableFile);
	
	int deleted = 0;
	
	if (whereAttr == NULL)
	{
		fclose(tableFile);
		
		tableFile = fopen(filePath, "w");
		
		fwrite(tableData, sizeof(tableData[0]), 3, tableFile);
		fwrite(attributesBlock, sizeof(attributesBlock[0]), tableData[0], tableFile);
	}
	else
	{
		int offset = 0;
		int attributeIndex = -1;
		
		for (int i = 0; i < tableData[0]; i++)
		{
			if (strcmp(attributesBlock[i].attribute.attributeName, whereAttr) == 0)
			{
				attributeIndex = i;
				break;
			}
			offset += attributesBlock[i].attribute.size;
		}
		
		if (attributeIndex != -1)
		{
			RecordBlock reader, empty;
			initRecordBlock(&reader, tableData[2]);
			initRecordBlock(&empty, tableData[2]);
			
			empty.flag = EMPTY;
			
			size_t recordBlockMemorySize = sizeOfRecordBlock(&reader);
			
			while(freadRecordBlock(&reader, tableFile) == 1)
			{
				if (memcmp(reader.data + offset, whereValue, attributesBlock[attributeIndex].attribute.size) == 0)  
				{
					fseek(tableFile, -recordBlockMemorySize, SEEK_CUR); 
					fwriteRecordBlock(&empty, tableFile);
					
					deleted++;
				}
			}
			
			delRecordBlock(&reader);
			delRecordBlock(&empty);
		}
	}
	
	fclose(tableFile);
	
	return deleted;
}

int dropDatabase(ParsedStringQueue **queue, AccountData *clientAccount)
{
	if (clientAccount->openningDatabase == 1 && strcasecmp(clientAccount->databaseName, (*queue)->parsedString) == 0)
	{
	
		char filePath[1024];
		sprintf(filePath, "%s/%s", __DATABASE_ROOT, (*queue)->parsedString); 
		DIR *databaseDirectory = opendir(filePath);
		if (databaseDirectory != NULL)
		{
			rmdir(filePath);
			
			char databaseName[64];
			memset(databaseName, 0, sizeof(databaseName));
			strcpy(databaseName, (*queue)->parsedString);
			
			
			Attribute attribute[__MAX_ATTRIBUTE_ON_TABLE];
			RecordBlockVector records;
			int attributeTotal = 0;
			int recordBlockSize = 0;
			initRecordBlockVector(&records);
			selectReadTable(
        		"admin", "database", &records, attribute, &attributeTotal, 
        		&recordBlockSize , "name", databaseName
        	);
        	
        	int result = 0;
			
			if (records.size == 1)
			{
				int databaseID = 0;
				memcpy(&databaseID, records.record[0].data, sizeof(databaseID));
				
				result = 1;
				
				deleteFromDatabaseTable("admin", "database", "name", databaseName);
				deleteFromDatabaseTable("admin", "database_permission", "databaseID", &databaseID);
				
				clientAccount->openningDatabase = 0;
			}
			
			delRecordBlockVector(&records);
			
			return result;
		}
		else
		{
			return 0;
		}
	}
	else
	{
		return 0;
	}
}

int dropTable(ParsedStringQueue **queue, AccountData *clientAccount)
{
	if (clientAccount->openningDatabase == 1)
	{
		char filePath[1024];
		sprintf(filePath, "%s/%s/%s", __DATABASE_ROOT, clientAccount->databaseName, (*queue)->parsedString);
		return remove(filePath);
	}
	
	return -1;
}

int dropColumn(ParsedStringQueue **queue,  AccountData *clientAccount)
{
	if (*queue == NULL || clientAccount->openningDatabase == 0)
	{
		return 0;
	}
	
	char columnName[__MAX_ATTRIBUTE_NAME_LENGTH];
	memset(columnName, 0, sizeof(columnName));
	strcpy(columnName, (*queue)->parsedString); 
	
	popParsedStringQueue(queue);
	
	if (*queue == NULL || strcasecmp((*queue)->parsedString, "FROM") != 0)
	{
		return 0;
	}
	
	popParsedStringQueue(queue);
		
	if (*queue == NULL)
	{
		return 0;
	}
	
	char tableName[64];
	strcpy(tableName, (*queue)->parsedString);

	char filePath[1024];
	char filePathForTemp[1024];
	sprintf(filePath, "%s/%s/%s", __DATABASE_ROOT, clientAccount->databaseName, tableName);
	strcpy(filePathForTemp, filePath);
	strcat(filePathForTemp, " temp");
	FILE *tableFile = fopen(filePath, "r");
	
	if (tableFile != NULL)
	{
		int tableData[3];
		fread(tableData, sizeof(tableData[0]), 3, tableFile);
		
		AttributeBlock attributesBlock[tableData[0]];
		fread(attributesBlock, sizeof(attributesBlock[0]), tableData[0], tableFile);
		
		int deletedAttributeIndex = -1;
		int deletedAttributeDataOffset = 0;
		int newSize = 0;
		
		for (int i = 0; i < tableData[0]; i++)
		{
			if (strcmp(attributesBlock[i].attribute.attributeName, columnName) == 0)
			{
				deletedAttributeIndex = i;
				deletedAttributeDataOffset = newSize;
			}
			else
			{
				newSize += attributesBlock[i].attribute.size;
			}
		}
		
		int returnValue = 0;
		
		if (deletedAttributeIndex != -1)
		{
			FILE *newFile = fopen(filePathForTemp, "w");
			int newTableData[3] = {tableData[0] - 1, tableData[1], newSize};
			fwrite(newTableData, sizeof(int), 3, newFile);
			
			if (deletedAttributeIndex != 0)
			{
				fwrite(attributesBlock, sizeof(attributesBlock[0]), deletedAttributeIndex, newFile);
			}
			if (deletedAttributeIndex != tableData[0] - 1)
			{
				fwrite(
					attributesBlock + sizeof(attributesBlock[0]) * (deletedAttributeIndex + 1), 
					sizeof(attributesBlock[0]), tableData[0] - 1 - deletedAttributeIndex, newFile
				);
			}
			
			RecordBlock readFromOldFile;
			RecordBlock writeToNewFile;
			initRecordBlock(&readFromOldFile, tableData[2]);
			initRecordBlock(&writeToNewFile, newSize);
			
			int postDeletedAttributeOffsetForWriteToNewFile = deletedAttributeDataOffset + attributesBlock[deletedAttributeIndex].attribute.size;
			int postDeletedAttributeSizeForWriteToNewFile = tableData[2] - postDeletedAttributeOffsetForWriteToNewFile; 
			
			while(freadRecordBlock(&readFromOldFile, tableFile) == 1)
			{
				if (readFromOldFile.flag == FILLED)
				{
					if (deletedAttributeIndex != 0)
					{
						memcpy(writeToNewFile.data, readFromOldFile.data, deletedAttributeDataOffset);
					}
					if (deletedAttributeIndex != tableData[0] - 1)
					{
						memcpy(
							writeToNewFile.data + deletedAttributeDataOffset, 
							readFromOldFile.data + postDeletedAttributeOffsetForWriteToNewFile, 
							postDeletedAttributeSizeForWriteToNewFile
						);
					}
					
					fwriteRecordBlock(&writeToNewFile, newFile);
				}
			}
			
			delRecordBlock(&readFromOldFile);
			delRecordBlock(&writeToNewFile);
			
			fclose(newFile);
			
			remove(filePath);
			rename(filePathForTemp, filePath);
		}
		
		fclose(tableFile);
		return 1;
	}	
	return 0;
}

int main(int argc, char **argv) 
{/*
	pid_t pidDaemonProcess = 0, sidDaemonProcess;
	
	pidDaemonProcess = fork();
	
	if (pidDaemonProcess < 0)
	{
		exit(EXIT_FAILURE);
	}
	else if (pidDaemonProcess > 0)
	{
		exit(EXIT_SUCCESS);
	}
	
	umask(0);
	
	sidDaemonProcess = setsid();
	
	if (sidDaemonProcess < 0)
	{
		exit(EXIT_FAILURE);
	}
	
	close(STDIN_FILENO);
	close(STDOUT_FILENO);
	close(STDERR_FILENO);
   	*/
   	 
    struct sockaddr_in newConnectionAddr;
    int serverFileDescriptor;
    int newConnectionFileDescriptor, temp_fd;
    socklen_t addrlen;
    
    struct epoll_event clientsList[__MAX_CONNECTIONS];
    struct epoll_event epollEventNewConnection;
    int epollEventCounter = 0;
    int epollFileDescriptor = epoll_create(__MAX_CONNECTIONS + 1);
    serverFileDescriptor = createTCPServerSocket(); 

    AccountData clientAccountData[__MAX_CONNECTIONS];
	memset(clientAccountData, 0, sizeof(clientAccountData));

    if (serverFileDescriptor == -1) 
    {
        return -1; 
    }   

    setupEpollConnection(epollFileDescriptor, serverFileDescriptor, &epollEventNewConnection);

    char message[__DATA_BUFFER];
    
    createDatabaseRoot();
    
    while (1) 
    {
        epollEventCounter = epoll_wait(epollFileDescriptor, clientsList, __MAX_CONNECTIONS, __SERVER_TIME_OUT_MSEC);

        for (int i = 0; i < epollEventCounter ; i++)
        {
            if (clientsList[i].data.fd == serverFileDescriptor) 
            { 
                newConnectionFileDescriptor = accept(serverFileDescriptor, (struct sockaddr*)&newConnectionAddr, &addrlen);

                if (newConnectionFileDescriptor >= 0) 
                {
                    setupEpollConnection(epollFileDescriptor, newConnectionFileDescriptor, &epollEventNewConnection);
                }
                else 
                {
                	printf("Failed to accept new connection\n");
                }
            }
            else if (clientsList[i].events & EPOLLIN && clientsList[i].data.fd >= 0) 
            {
                recv(clientsList[i].data.fd, message, __DATA_BUFFER, 0);
                if (message[0] == 'L')
                {
                	int usernameLength;
                	int passwordLength;
                	size_t intSize = sizeof(usernameLength);
                	
                	memcpy(&usernameLength, message + 1, intSize);
                	memcpy(&passwordLength, message + 1 + intSize, intSize);
                	
                	char username[64];
                	char password[64];
                	
                	memset(username, 0, 64);
                	memset(password, 0, 64);
                	
                	memcpy(username, message + 1 + intSize * 2, usernameLength);
                	memcpy(password, message + 1 + intSize * 2 + usernameLength, passwordLength);
                	
                	RecordBlockVector records;
                	initRecordBlockVector(&records);
                	Attribute attribute[__MAX_ATTRIBUTE_ON_TABLE];
                	int attributeTotal = 0;
                	int recordBlockSize = 0;
                	
                	char usernameAttribute[64];
                	memset(usernameAttribute, 0, sizeof(usernameAttribute));
                	strcpy(usernameAttribute, "username");
                	
                	selectReadTable(
                		"admin", "account", &records, attribute, &attributeTotal, 
                		&recordBlockSize , usernameAttribute, username
                	);
                	
                	if (records.size == 1) 
                	{
            			if (memcmp(password, records.record[0].data + attribute[0].size + attribute[1].size, attribute[2].size) == 0)
            			{
		            		strcpy(message, "success");
		            		send(clientsList[i].data.fd, message, __DATA_BUFFER, 0);
		            		
		            		memcpy(&(clientAccountData[i].id), records.record[0].data, sizeof(clientAccountData[i].id));
		            		clientAccountData[i].openningDatabase = 0;
                		}
                		else
                		{
		            		strcpy(message, "failed");
		            		send(clientsList[i].data.fd, message, __DATA_BUFFER, 0);
                		}
                	}
                	else
                	{
                		strcpy(message, "failed");
                		send(clientsList[i].data.fd, message, __DATA_BUFFER, 0);
                	}
                	
                	delRecordBlockVector(&records);
                }
                else if (strcmp(message, "root") == 0)
                {
                	clientAccountData[i].id = 0;
                	clientAccountData[i].openningDatabase = 0;
                }
                else
                {
                	ParsedStringQueue *queue = parseStringSQLScript(message);
                	
                	if (queue != NULL && strcasecmp(queue->parsedString, "CREATE") == 0)
                	{
	            		popParsedStringQueue(&queue);
	            		
		            	if (queue != NULL && strcasecmp(queue->parsedString, "USER") == 0 && clientAccountData[i].id == 0)
                		{
	            			popParsedStringQueue(&queue);
	            			if (createNewAccount(&queue) == 1)
	            			{
	            				strcpy(message, "MBerhasil menambahkan akun");
	            			}
	            			else
	            			{
	            				strcpy(message, "MGagal menambahkan akun");
	            			}
	            		}
	            		else if (queue != NULL && strcasecmp(queue->parsedString, "DATABASE") == 0)
                		{
	            			popParsedStringQueue(&queue);
	            			if (createNewDatabase(&queue, clientAccountData[i].id) == 1)
	            			{
	            				strcpy(message, "MBerhasil membuat database baru");
	            			}
	            			else
	            			{
	            				strcpy(message, "MGagal membuat database baru");
	            			}
	            		}
	            		else if (queue != NULL && strcasecmp(queue->parsedString, "TABLE") == 0)
                		{
	            			popParsedStringQueue(&queue);
	            			if (createTableByUser(&queue, &(clientAccountData[i])) == 1)
	            			{
	            				strcpy(message, "MBerhasil membuat table baru");
	            			}
	            			else
	            			{
	            				strcpy(message, "MGagal membuat table baru");
	            			}
	            		}
	            		else
	            		{
        					strcpy(message, "MScript error");
	            		}
		            }
		            else if (queue != NULL && strcasecmp(queue->parsedString, "DROP") == 0)
                	{
	            		popParsedStringQueue(&queue);
	            		
		            	if (queue != NULL && strcasecmp(queue->parsedString, "DATABASE") == 0)
                		{
	            			popParsedStringQueue(&queue);
	            			if (dropDatabase(&queue, &clientAccountData[i]) == 1)
	            			{
	            				strcpy(message, "MBerhasil drop database");
	            			}
	            			else
	            			{
	            				strcpy(message, "MGagal drop database");
	            			}
	            		}
	            		else if (queue != NULL && strcasecmp(queue->parsedString, "TABLE") == 0)
                		{
	            			popParsedStringQueue(&queue);
	            			if (dropTable(&queue, &(clientAccountData[i])) == 0)
	            			{
	            				strcpy(message, "MBerhasil drop table");
	            			}
	            			else
	            			{
	            				strcpy(message, "MGagal drop table");
	            			}
	            		}
	            		else if (queue != NULL && strcasecmp(queue->parsedString, "COLUMN") == 0)
                		{
	            			popParsedStringQueue(&queue);
	            			if (dropColumn(&queue, &(clientAccountData[i])) == 1)
	            			{
	            				strcpy(message, "MBerhasil drop column");
	            			}
	            			else
	            			{
	            				strcpy(message, "MGagal drop column");
	            			}
	            		}
	            		else
	            		{
        					strcpy(message, "MScript error");
	            		}
		            }
		            else if (queue != NULL && strcasecmp(queue->parsedString, "GRANT") == 0)
                	{
	            		popParsedStringQueue(&queue);
	            		
		            	if (queue != NULL && strcasecmp(queue->parsedString, "PERMISSION") == 0 && clientAccountData[i].id == 0)
                		{
	            			popParsedStringQueue(&queue);
	            			if (grantPermissionUserOnDatabase(&queue) == 1)
	            			{
	            				strcpy(message, "MBerhasil menambahkan permission");
	            			}
	            			else
	            			{
	            				strcpy(message, "MGagal menambahkan permission");
	            			}
	            		}
	            		else
	            		{
        					strcpy(message, "MScript error");
	            		}
		            }
		            else if (queue != NULL && strcasecmp(queue->parsedString, "USE") == 0)
                	{
	            		popParsedStringQueue(&queue);
	            		
	            		if (useDatabase(&queue, &(clientAccountData[i])) == 1)
	            		{
            				strcpy(message, "MBerhasil membuka database");
            			}
            			else
            			{
            				strcpy(message, "MGagal membuka database");
            			}
		            }
		            else if (queue != NULL && strcasecmp(queue->parsedString, "INSERT") == 0)
                	{
	            		popParsedStringQueue(&queue);
	            		
		            	if (queue != NULL && strcasecmp(queue->parsedString, "INTO") == 0)
                		{
	            			popParsedStringQueue(&queue);
	            			if (insertIntoDatabaseScript(&queue, &clientAccountData[i]) == 1)
	            			{
	            				strcpy(message, "MBerhasil memasukkan data");
	            			}
	            			else
	            			{
	            				strcpy(message, "MGagal memasukkan data");
	            			}
	            		}
	            		else
	            		{
        					strcpy(message, "MScript error");
	            		}
		            }
		            else
		            {
        				strcpy(message, "MScript error");
		            }
                	send(clientsList[i].data.fd, message, __DATA_BUFFER, 0);
                	
                	while(queue != NULL)
                	{
                		popParsedStringQueue(&queue);
                	}
                }
            }
        }
    }

    for (int i = 0; i < __MAX_CONNECTIONS; i++) 
    {
        if (clientsList[i].data.fd > 0) 
        {
            close(clientsList[i].data.fd);
        }
    }
    return 0;
}
