#include <iostream>
#include <string>
#include <vector>
#include <array>
#include <deque>
#include <thread>
#include <mutex>
#include <chrono>
#include <cstdlib>
#include <unistd.h>
#include <atomic>
#include <map>
#include "message_base.h"
#include "common/json.hpp"
#define NUM_SERVERS 5
using namespace std;
using json = nlohmann::json;

vector<string> parse(string command, char delimiter = ' '){
    vector<string> str_list{};
    size_t pos = 0;

    int str_cnt = 0;
    while ((pos = command.find(delimiter)) != string::npos && (str_cnt++)<5) {
        if(pos!=0){
            str_list.push_back(command.substr(0, pos));
        }
        command.erase(0, pos + 1);
    }
    str_list.push_back(command.substr(0, command.length()));
    return str_list;
}


namespace Client{

    void reply_ok(){
        cout << "OK" << endl;
    }

    void reply_reject(){
        // If a query is made to an account that has not previously received a deposit, the client should print NOT FOUND, ABORTED and abort the transaction.
        //  If the account does not exist (i.e, has never received any deposits), the client should print NOT FOUND, ABORTED and abort the transaction.
        cout << "NOT FOUND, ABORTED" << endl;
    }

    void reply_commit(){
        cout << "COMMIT OK" << endl;
    }

    void reply_abort(){
        // The client should reply with ABORTED to confirm that the transaction was aborted.
        cout << "ABORTED" << endl;
    }

}

deque<json> cli_command_queue;
mutex cli_command_queue_mtx;
string client_id;
message_base::MessageBaseClient client;
vector<message_base::ServerInfo> sinfo;


void cli_rpc_worker(){
    while(true)
    {
        sleep(0.001);
        // other RPCs, abort will never be in the queue
        cli_command_queue_mtx.lock();
        if(!cli_command_queue.empty()){
            json rpc = cli_command_queue.front();
            cli_command_queue_mtx.unlock();

            DEBUG_INFO(rpc.dump()+" In thread");
            if(rpc["type"].get<string>()==message_base::DEPOSIT){
                // send RPC to server
                if(client.unicast(rpc["serverID"],rpc)){
                    // wait for reply
                    json rpl = client.client_recv(rpc["serverID"]);
                    if(rpl.contains("state") && rpl["state"].get<bool>()) {
                        Client::reply_ok();
                    }
                }
            }
            else if(rpc["type"].get<string>()==message_base::BALANCE){
                // send RPC to server
                if(client.unicast(rpc["serverID"],rpc)){
                    // wait for reply of balance
                    json rpl = client.client_recv(rpc["serverID"]);
                    if(rpl.contains("balance") && rpl.contains("state") && rpl["state"].get<bool>()){
                        cout << rpc["serverID"].get<string>() + "." + rpc["account"].get<string>() << " = " << rpl["balance"] << endl;
                    }
                    else{
                        Client::reply_reject();
                    }
                }
            }
            else if(rpc["type"]==message_base::WITHDRAW){
                // send RPC to server
                if(client.unicast(rpc["serverID"],rpc)){
                    // wait for reply of withdraw state
                    json rpl = client.client_recv(rpc["serverID"].get<string>());
                    if(rpl.contains("state") && rpl["state"].get<bool>()){
                        Client::reply_ok();
                    }else{
                        Client::reply_reject();
                    }
                }
            }
            else if (rpc["type"].get<string>()==message_base::COMMIT){
                // send RPC to server
                if(client.multicast(rpc)) {
                    // wait for 5 servers to commit/abort
                    int num_replies = 0;
                    bool can_commit = true;
                    while(num_replies<NUM_SERVERS){
                        json rpl = client.client_recv(sinfo[num_replies].server_identifier);
                        if (rpl.contains("state") && !rpl["state"].get<bool>()) {
                            can_commit = false; // is any of them is false (abort)
                        }
                        num_replies++;
                    }
                    if(can_commit){
                        rpc["CP_NUM"] = 2;
                        rpc["CP_STATE"] = true;
                        client.multicast(rpc);
                        Client::reply_ok();
                    }else{
                        rpc["CP_NUM"] = 2;
                        rpc["CP_STATE"] = false;
                        client.multicast(rpc);
                        Client::reply_abort();
                    }
                }
            }
            cli_command_queue_mtx.lock();
            cli_command_queue.pop_front();
            cli_command_queue_mtx.unlock();
        }
        else{
            cli_command_queue_mtx.unlock();
        }
    }
}

// client
int main(int argc, char const *argv[]) {
    string config_file;
    if(argc==3){
        client_id = argv[1];
        config_file = argv[2];

        ifstream configfilestream(config_file);
        string node_identifier, node_address;
        unsigned int port_no;
        // Read the node config file and prepare the required data structures related to the nodes
        if (configfilestream.is_open()) {
            cout << "config file open successful" << endl;
            for (int i = 0; i < NUM_SERVERS; ++i) {
                message_base::ServerInfo temp_sinfo;
                configfilestream >> node_identifier >> node_address >> port_no;
                cout << node_identifier << ' ' << node_address << ' ' << port_no << endl;
                temp_sinfo.server_identifier = string(node_identifier);
                temp_sinfo.server_address = HostToIp(string(node_address));
                temp_sinfo.server_port = port_no;
                sinfo.push_back(temp_sinfo);
            }
        }
    } else {
        cout << "config file open error" << endl;
        return 0;
    }

    // automatically connect to all the necessary servers
    client = message_base::MessageBaseClient(sinfo);

    thread cli_rpc_thread(cli_rpc_worker);
    auto cli_rpc_thread_handle = cli_rpc_thread.native_handle();
    cli_rpc_thread.detach();
    //int phase;

    DEBUG_INFO("start accepting commands typed in by the user");
    // start accepting commands typed in by the user
    string command;
    while(getline(cin, command))
    {
        // You should ignore any commands occuring outside a transaction (other than BEGIN).
        if(command == message_base::BEGIN)
        {
            // BEGIN: Open a new transaction, and reply with “OK”.
            Client::reply_ok();
            while(getline(cin, command))
            {
                DEBUG_INFO("Get from CLI: "+command);
                auto str_list = parse(command);
                // DEPOSIT server.account amount: Deposit some amount into an account. Amount will be a positive integer. (You can assume that the value of any account will never exceed 1,000,000,000.) The account balance should increase by the given amount. If the account was previously unreferenced, it should be created with an initial balance of amount. The client should reply with OK
                // BALANCE server.account: The client should display the current balance in the given account. If a query is made to an account that has not previously received a deposit, the client should print NOT FOUND, ABORTED and abort the transaction.
                // WITHDRAW server.account amount: Withdraw some amount from an account. The account balance should decrease by the withdrawn amount. The client should reply with OK if the operation is successful. If the account does not exist (i.e, has never received any deposits), the client should print NOT FOUND, ABORTED and abort the transaction.
                // COMMIT: Commit the transaction, making its results visible to other transactions. The client should reply either with COMMIT OK or ABORTED, in the case that the transaction had to be aborted during the commit process.
                // ABORT: Abort the transaction. All updates made during the transaction must be rolled back. The client should reply with ABORTED to confirm that the transaction was aborted.
                if(str_list[0]==message_base::DEPOSIT || str_list[0]==message_base::BALANCE || str_list[0]==message_base::WITHDRAW || str_list[0]==message_base::COMMIT || str_list[0]==message_base::ABORT){
                    json rpc;
                    if(str_list[0]==message_base::DEPOSIT){
                        auto server_account_str = parse(str_list[1],'.');
                        pair<string,string> server_account_pair(server_account_str[0],server_account_str[1]);
                        int amount = stoi(str_list[2]);
                        rpc = json{{"clientID", client_id},
                                   {"serverID", server_account_pair.first},
                                   {"type", message_base::DEPOSIT},
                                   {"account", server_account_pair.second},
                                   {"amount", amount},};
                    }
                    else if(str_list[0]==message_base::BALANCE){
                        auto server_account_str = parse(str_list[1],'.');
                        pair<string,string> server_account_pair(server_account_str[0],server_account_str[1]);

                        rpc = json{{"clientID", client_id},
                                   {"serverID", server_account_pair.first},
                                   {"type", message_base::BALANCE},
                                   {"account", server_account_pair.second},};

                    }
                    else if(str_list[0]==message_base::WITHDRAW){
                        auto server_account_str = parse(str_list[1],'.');
                        pair<string,string> server_account_pair(server_account_str[0],server_account_str[1]);
                        int amount = stoi(str_list[2]);
                        rpc = json{{"clientID", client_id},
                                   {"serverID", server_account_pair.first},
                                   {"type", message_base::WITHDRAW},
                                   {"account", server_account_pair.second},
                                   {"amount", amount},};

                    }
                    else if (str_list[0]==message_base::COMMIT){
                        rpc = json{{"clientID", client_id},
                                   {"CP_NUM", 1},
                                   {"CP_STATE", true}, // true means can commit
                                   {"type", message_base::COMMIT}};

                    }
                    else if (str_list[0]==message_base::ABORT){
                        // if any of the rpc is abort, we choose to send this abort RPC immediately and pop all the other RPCs in the RPC queue at client side.

                        rpc = json{{"clientID", client_id},
                                   {"type", message_base::ABORT}};
                        // now ABORT is at the front
                        cli_command_queue_mtx.lock();
                        cli_command_queue.clear();
                        cli_command_queue_mtx.unlock();

                        pthread_cancel(cli_rpc_thread_handle); // stop waiting the response of the server because it already responsed or never response
                        cli_command_queue_mtx.unlock(); // unlock the mutex anyway
                        DEBUG_INFO("ABORT AND MULTICAST");
                        // send RPC to server
                        if (client.multicast(rpc)) {
                            int num_replies = 0;
                            // wait for 5 servers to abort
                            while(num_replies<NUM_SERVERS){
                                json rpl = client.client_recv(sinfo[num_replies].server_identifier);
                                if (rpl.contains("state") && rpl["state"].get<bool>()) {
                                    DEBUG_INFO("RECEIVE FROM "+sinfo[num_replies].server_identifier);
                                }
                                num_replies++;
                            }
                        }

                        cli_rpc_thread = thread(cli_rpc_worker);
                        cli_rpc_thread_handle = cli_rpc_thread.native_handle();
                        cli_rpc_thread.detach();
                        Client::reply_abort();
                        break;
                    }
                    // push the cli command json rpc to the queue
                    if(str_list[0]!=message_base::ABORT){
                        cli_command_queue_mtx.lock();
                        cli_command_queue.push_back(rpc);
                        cli_command_queue_mtx.unlock();
                    }

                    if(str_list[0]==message_base::COMMIT||str_list[0]==message_base::ABORT){
                        break;
                    }
                }
            }
        }
    }

}
