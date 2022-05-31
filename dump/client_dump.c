#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "strings.h"
#include "ctype.h"
#include "unistd.h"
#include "dirent.h"
#include "errno.h"

#include "netdb.h"
#include "netinet/in.h" 

#include "sys/stat.h"
#include "sys/types.h"
#include "sys/epoll.h"

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

#ifndef __STRING_MAX_LENGTH
	#define __STRING_MAX_LENGTH 655356
#else
	#error __STRING_MAX_LENGTH already defined
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
	message[0] = 'A';
	int usernameLength = strlen(username);
	int passwordLength = strlen(password);
	size_t integerSize = sizeof(usernameLength);
	memcpy(&message[1], &usernameLength, sizeof(usernameLength));
	memcpy(&message[1 + integerSize], &passwordLength, sizeof(passwordLength));
	sprintf(&message[1 + 2 * integerSize], "%s%s", username, password);
}

void printMessage(char message[])
{
	printf("%s\n", message);
}

int main(int argc, char **argv)
{
	if (argc < 6 || strcmp(argv[1], "-u") != 0 || strcmp(argv[3], "-p") != 0) 
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

	while (1)
	{
		recv(socketConnectionFileDescriptor, message, sizeof(message), 0);
		if (strlen(message) > 0)
		{
			if (message[0] == 'I' || message[0] == 'S' || message[0] == 'U' || message[0] == 'D')
				printMessage(message);
		}
	}
}
