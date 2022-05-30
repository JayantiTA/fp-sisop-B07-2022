#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "time.h"
#include "errno.h"
#include "unistd.h"
#include "netdb.h"
#include "netinet/in.h"
#include "sys/time.h"

#ifndef __DATA_BUFFER
	#define __DATA_BUFFER 4096
#else
	#error __DATA_BUFFER already defined
#endif

#ifndef __SERVER_PORT
	#define __SERVER_PORT 1122 
#else
	#error __SERVER_PORT already defined
#endif

#ifndef __ROOT_ID
	#define __ROOT_ID 0
#else
	#error __ROOT_ID already defined
#endif

int establishedConnection() 
{
	struct sockaddr_in socketAddress;
	int socketFileDescriptor;
	struct hostent *localHost;
	
	socketFileDescriptor = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (socketFileDescriptor == -1) {
		fprintf(stderr, "Error: [%s]\n", strerror(errno));
		return -1;
	}
	
	socketAddress.sin_family = AF_INET;
	socketAddress.sin_port = htons(__SERVER_PORT);
	localHost = gethostbyname("localhost");
	socketAddress.sin_addr = *((struct in_addr *)localHost->h_addr);
	
	if (connect(socketFileDescriptor, (struct sockaddr *)&socketAddress, sizeof(socketAddress)) == -1)
	{
		fprintf(stderr, "Error: [%s]\n", strerror(errno));
		close(socketFileDescriptor);
		return -1;
	} 
	
	return socketFileDescriptor;
}

void constructLoginMessage(char message[], char username[], char password[])
{
	message[0] = 'L';
	int usernameLength = strlen(username);
	int passwordLength = strlen(password);
	size_t integerSize = sizeof(usernameLength);
	memcpy(&message[1], &usernameLength, sizeof(usernameLength));
	memcpy(&message[1 + integerSize], &passwordLength, sizeof(passwordLength));
	sprintf(&message[1 + 2 * integerSize], "%s%s", username, password);
}

void writeLog(char username[], char message[])
{
	FILE *fileWriterLog = fopen("./client.log", "a+");
	time_t currentTime = time(NULL);
	struct tm currentLocalTime = *localtime(&currentTime);
	char logText[4096];
	
	sprintf(logText, "%04d-%02d-%02d %02d:%02d:%02d:%s:%s",
		currentLocalTime.tm_year + 1900,
		currentLocalTime.tm_mon,
		currentLocalTime.tm_mday, 
		currentLocalTime.tm_hour,
		currentLocalTime.tm_min,
		currentLocalTime.tm_sec,
		username,
		message
	);

	fprintf(fileWriterLog, "%s\n", logText);
	
	fclose(fileWriterLog);
}

int main(int argc, char** argv) 
{
	if (getuid() != __ROOT_ID && (argc < 5 || strcmp(argv[1], "-u") != 0 || strcmp(argv[3], "-p") != 0)) 
	{
		fprintf(stderr, "Error, login command '-u [username] -p [password]'\n");
		exit(EXIT_FAILURE);
	}
	int socketConnectionFileDescriptor = establishedConnection();
	if (socketConnectionFileDescriptor == -1)
	{
		exit(EXIT_FAILURE);
	}
	
	char message[__DATA_BUFFER];
	char command[__DATA_BUFFER];
	char username[64];
	if (getuid() == __ROOT_ID)
	{
		strcpy(message, "root");
		strcpy(username, message);
		send(socketConnectionFileDescriptor, message, sizeof(message), 0);
	}
	else
	{
		strcpy(username, argv[2]);
		
		constructLoginMessage(message, argv[2], argv[4]);
		send(socketConnectionFileDescriptor, message, sizeof(message), 0);
		recv(socketConnectionFileDescriptor, message, sizeof(message), 0);
		
		if (strcmp(message, "success") != 0) 
		{
			fprintf(stderr, "Login failed\n");
			exit(EXIT_FAILURE);
		}
		else
		{
			printf("Login success as %s\n", username);
		}
	}
	
	while(1)
	{
		scanf(" %[^\n]", message);
		if (message != NULL)
		{
			strcpy(command, message);
		}
		send(socketConnectionFileDescriptor, message, sizeof(message), 0);
		recv(socketConnectionFileDescriptor, message, sizeof(message), 0);
		
		if (message[0] == 'M')
		{
			printf("%s\n", message + 1);
			if (message[1] == 'B')
			{
				writeLog(username, command);
			}
		}
		else if (message[0] == 'Q')
		{
			recv(socketConnectionFileDescriptor, message, sizeof(message), 0);
			
			int totalAttribute = 0;
			memcpy(&totalAttribute, message, sizeof(totalAttribute));
			
			printf("|");
			for (int i = 0; i < totalAttribute; i++)
			{
				recv(socketConnectionFileDescriptor, message, sizeof(message), 0);
				printf("%10.10s|", message);
			}
			printf("\n|");
			for (int i = 0; i < totalAttribute; i++)
			{
				printf("----------|");
			}
			
			recv(socketConnectionFileDescriptor, message, sizeof(message), 0);
			int totalRecord = 0;
			memcpy(&totalRecord, message, sizeof(totalRecord));
			
			int finished = 0;
			int printOut = 0;
			
			printf("\n|");
			while(finished == 0)
			{
				recv(socketConnectionFileDescriptor, message, sizeof(message), 0);
				if (message[0] == 'C')
				{
					printOut = 0;
				}
				else if (message[0] == 'R')
				{
					printf("\n|");
				}
				else if (message[0] == 'V' && printOut == 0)
				{
					printf("%10.10s|", message + 1);
					printOut = 1;
				}
				else if (message[0] == 'F')
				{
					finished = 1;
				}
			}
			
			for (int i = 0; i < totalAttribute; i++)
			{
				printf("----------|");
			}
			printf("\n");
		}
	}
	return 0;
}
