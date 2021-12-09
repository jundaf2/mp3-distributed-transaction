/* message passing classes based on TCP/IP and socket for central logging server / clients */
/* Reference website: https://www.geeksforgeeks.org/socket-programming-cc/#:~:text=Socket%20Programming%20in%20C%2FC%2B%2B.%20After%20creation%20of%20the,we%20use%20INADDR_ANY%20to%20specify%20the%20IP%20address. */
#pragma once
#ifndef MESSAGEBASE
#define MESSAGEBASE

#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>
#include <cstring>
#include <vector>
#include <array>
#include <list>
#include <queue>
#include <thread>
#include <pthread.h>
#include <mutex>
#include <atomic>
#include <map>
#include <unistd.h>
#include <iomanip>
#include <algorithm>
#include "common/json.hpp"
using json = nlohmann::json;
using namespace std;

void DEBUG_INFO(string info){
    cout << "\x1b[34m" << info << "\x1b[0m" << endl;
}

#include <string>

#include <netdb.h>
#include <arpa/inet.h>

std::string HostToIp(const std::string& host) {
    hostent* hostname = gethostbyname(host.c_str());
    if(hostname)
        return std::string(inet_ntoa(**(in_addr**)hostname->h_addr_list));
    return {};
}

namespace message_base {
    constexpr unsigned int PORT = 1234;
    constexpr char ADDRESS[] = "0.0.0.0";
    constexpr unsigned int BUFFER_SIZE = 5*1024*1024;
    const string BEGIN = "BEGIN";
    const string DEPOSIT = "DEPOSIT";
    const string BALANCE = "BALANCE";
    const string WITHDRAW = "WITHDRAW";
    const string COMMIT = "COMMIT";
    const string ABORT = "ABORT";

    struct NodeConnection {   // Declare connection struct type
        string node_identifier = "node";
        int send_recv_socket_fd = 0;
    };

    struct ServerInfo {   // Declare connection struct type
        string server_identifier = "A";
        string server_address{};
        unsigned int server_port = 1234;
    };

    class MessageBaseClient {
        private:
            struct sockaddr_in addr[10];

        public:
            vector<NodeConnection> nodes_connection_group;
            vector<ServerInfo> server_infos;

            MessageBaseClient() = default;
            MessageBaseClient(vector<ServerInfo> sinfo) : server_infos(sinfo) {
                for(int i=0; i<sinfo.size();i++){
                    NodeConnection nc_new;
                    nc_new.node_identifier = server_infos[i].server_identifier;
                    nodes_connection_group.push_back(nc_new);
                }

                /*
                 * as well as initiate a TCP connection to each of the other nodes
                 */

                for(auto& ncg: nodes_connection_group) {
                    int new_socket = socket(PF_INET, SOCK_STREAM, 0);
                    if (new_socket < 0) {
                        perror("socket creation failed");
                        exit(EXIT_FAILURE);
                    } else {
                        ncg.send_recv_socket_fd = new_socket;
                    }
                }
                /*
                 * Node’s implementation should continuously try to initiate connections until successful to ensure that the
                 * implementation appropriately waits for a connection to be successfully established before trying to send on it.
                 */
                // connect all
                int connected_cnt = 0;
                vector<bool> all_connected_flag(server_infos.size(),false);
                while(connected_cnt<server_infos.size()){
                    for(int i=0;i<server_infos.size();i++) {
                        if(!all_connected_flag[i]){
                            /*---- Configure settings of the server address struct ----*/
                            addr[i].sin_family = AF_INET;
                            addr[i].sin_port = htons(server_infos[i].server_port); // force port
                            // Convert IPv4 and IPv6 addresses from text to binary form
                            if (::inet_pton(AF_INET, server_infos[i].server_address.c_str(), &addr[i].sin_addr) <= 0) {
                                perror((string("invalid address / address not supported") + string(" --> ") + server_infos[i].server_address).c_str());
                                exit(EXIT_FAILURE);
                            }
                            if (::connect(nodes_connection_group[i].send_recv_socket_fd, (struct sockaddr *) &addr[i], sizeof(addr[i])) < 0) {
                                perror("\nConnection Failed \n");
                                cout << "Client fails to connect to "+server_infos[i].server_address<< endl;
                                //exit(EXIT_FAILURE);
                            }
                            else{
                                DEBUG_INFO("Connect to server "+server_infos[i].server_identifier);
                                ++connected_cnt;
                                all_connected_flag[i] = true;
                            }
                        }
                    }
                }
            }

            int get_socket_fd_by_node_id(string nid){
                for(int i=0; i<nodes_connection_group.size();i++){
                    if(nodes_connection_group[i].node_identifier==nid){
                        return nodes_connection_group[i].send_recv_socket_fd;
                    }
                }
                return -1;
            }

            bool unicast(string server_identifier, const json &j) {
                DEBUG_INFO("Unicast to server "+server_identifier);
                int send_recv_socket_fd = this->get_socket_fd_by_node_id(server_identifier);
                string msg = j.dump();
                if (::send(send_recv_socket_fd, (const void *) msg.c_str(), msg.length(), 0) < 0) {
                    printf("unicast message error: %s(errno: %d)\n", strerror(errno), errno);
                    return false;
                }
                return true;
            }

            bool multicast(const json &j) {
                for(auto& nc:nodes_connection_group){
                    if (!unicast(nc.node_identifier, j))
                    {
                        cout << "cast failed" << endl;
                    }
                }
                return true;
            }

            json client_recv(const string server_identifier){
                int send_recv_socket_fd = this->get_socket_fd_by_node_id(server_identifier);
                int numbytes;
                char buffer[message_base::BUFFER_SIZE];
                // block until receive
                if((numbytes = ::recv(send_recv_socket_fd, buffer, message_base::BUFFER_SIZE, 0))>0)
                {
                    buffer[numbytes] = '\0';
                    return json::parse(buffer);
                }
            }
    };

    class MessageBaseServer {
        private:
            array<thread,10> client_thread;
            struct sockaddr_in self_addr;
            struct sockaddr_in addr[10];

        public:
            array<NodeConnection,10> nodes_connection_group;
            vector<ServerInfo> server_infos;
            int listen_socket_fd;
            int num_clients = 0;

            MessageBaseServer() = default;
            MessageBaseServer(string this_server_id, vector<ServerInfo> sinfo) : server_infos(sinfo)
            {
                /*
                 *  Each server must listen for TCP connections from other nodes
                 */
                /*---- Create the socket: internet domain, dtream socket, Default protocol (TCP) ----*/
                unsigned int port = 1234;
                for (auto& server_info:server_infos){
                    if(server_info.server_identifier==this_server_id){
                        port = server_info.server_port;
                    }
                }

                listen_socket_fd = socket(PF_INET, SOCK_STREAM, 0);

                int opt = 1;

                if (listen_socket_fd == 0) {
                    perror("socket failed");
                    exit(EXIT_FAILURE);
                }

                /*---- optional, but helps in reuse of address and port ----*/
                if (setsockopt(listen_socket_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
                    perror("setsockopt");
                    exit(EXIT_FAILURE);
                }

                /*---- Configure settings of the server address struct ----*/
                self_addr.sin_family = AF_INET;
                self_addr.sin_port = htons(port); // force port
                self_addr.sin_addr.s_addr = inet_addr(ADDRESS); // local host
                memset(self_addr.sin_zero, '\0', sizeof(self_addr.sin_zero)); // padding '\0'

                /*----  binds the socket to the address and port number specified ----*/
                if (bind(listen_socket_fd, (struct sockaddr *) &self_addr, sizeof(self_addr)) < 0) {
                    perror("bind failed");
                    exit(EXIT_FAILURE);
                }

                /*---- Listen on the socket, with 10 max connection requests queued ----*/
                if (listen(listen_socket_fd, 10) < 0) {
                    perror("listen failed");
                    exit(EXIT_FAILURE);
                }
            }

            int get_socket_fd_by_node_id(string nid){
                for(int i=0; i<nodes_connection_group.size();i++){
                    if(nodes_connection_group[i].node_identifier==nid){
                        return nodes_connection_group[i].send_recv_socket_fd;
                    }
                }
                return -1;
            }



            void server_start(void (*worker)(NodeConnection*)){
                while(true)
                {
                    socklen_t addrlen = sizeof(addr);
                    struct sockaddr_in client_addr;
                    int new_sock = accept(listen_socket_fd, (struct sockaddr *) &client_addr, (socklen_t*)&addrlen);
                    if (new_sock<0)
                    {
                        perror("accept failed");
                        exit(EXIT_FAILURE);
                    }
                    else
                    {
                        DEBUG_INFO("Accept Client Connection!");
                        this->num_clients++;
                        inet_ntoa(client_addr.sin_addr);
                        NodeConnection nc_new;
                        nc_new.send_recv_socket_fd = new_sock;
                        nodes_connection_group[this->num_clients-1] = nc_new;
                        client_thread[this->num_clients-1] = thread(worker, &nodes_connection_group[this->num_clients-1]);
                        client_thread[this->num_clients-1].detach();
                    }

                }
            }

            bool unicast(string client_identifier, const json &j) {
                DEBUG_INFO("Unicast to client "+client_identifier);
                int send_recv_socket_fd = this->get_socket_fd_by_node_id(client_identifier);
                string msg = j.dump();
                if (::send(send_recv_socket_fd, (const void *) msg.c_str(), msg.length(), 0) < 0) {
                    printf("unicast message error: %s(errno: %d)\n", strerror(errno), errno);
                    return false;
                }
                return true;
            }

            bool multicast( const json &j) {
                for(auto& nc:nodes_connection_group){
                    if (!unicast(nc.node_identifier, j))
                    {
                        cout << "cast failed" << endl;
                    }
                }
                return true;
            }
    };
}

#endif

