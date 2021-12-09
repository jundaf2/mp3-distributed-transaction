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
#include "common/rwlock.hpp"
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


struct Balance{
    private:
        int amount = 0;
        rwlock::ReadWriteLock rw_mutex;
        string read_lock_holder = "";
        string write_lock_holder = "";
    public:
        Balance(int am=0) {
            amount = am;
        }
        ~Balance(){
            rw_mutex.readUnLock();
            rw_mutex.writeUnLock();
        }
        
        void roll_back(int tot_am, string client_id){
            int current_amount = this->amount;
            DEBUG_INFO("BEFORE ROLEBACK "+ to_string(current_amount));
            this->amount = current_amount - tot_am;
            DEBUG_INFO("AFTER ROLEBACK "+ to_string(this->amount));
        }

        void increase(int am, string client_id){
            if(this->write_lock_holder != client_id){
                rw_mutex.writeLock();
                this->write_lock_holder = client_id;
            }
            int current_amount = this->amount;
            DEBUG_INFO("READ SUCCESS");
            this->amount = current_amount + am;
            DEBUG_INFO("WRITE SUCCESS");
        }

        void decrease(int am, string client_id){
            if(this->write_lock_holder != client_id){
                rw_mutex.writeLock();
                this->write_lock_holder = client_id;
            }
            int current_amount = this->amount;
            this->amount = current_amount - am;
        }

        bool check_positive(){
            if(this->amount>0) return true;
            else return false;
        }

        bool check_negative(){
            if(this->amount<0) return true;
            else return false;
        }

        int getAmount(string client_id){ // for client read
            if(this->write_lock_holder != client_id){
                rw_mutex.readLock();
            }
            return this->amount;
        }

        int getAmount(){ // for self read
            return this->amount;
        }

        void release_locks(){
            this->read_lock_holder = "";
            this->write_lock_holder = "";
            rw_mutex.writeUnLock();
            rw_mutex.readUnLock();
        }
};

// A transaction should see its own tentative updates
class Transactions{
    private:
        map<string, Balance> account_balance;
        vector<string> account_permanent;
        map<string, map<string,int>> client_transaction__account_amounts;
    public:
        Transactions() = default;

        bool check(){
            for (auto &acc_bal_pair: this->account_balance){
                if (acc_bal_pair.second.check_negative())
                    return false;
            }
            return true;
        }

        void update_account_list(){
            account_permanent.clear();
            for(auto& acc_bal: this->account_balance){
                this->account_permanent.push_back(acc_bal.first);
            }
        }

        bool deposit(string server_account, int deposit_amount, string client_id){
            // current transaction records
            if(!(this->client_transaction__account_amounts.count(client_id)>0)){
                pair<string,int> account_amounts_pair(server_account,deposit_amount);
                pair<string, map<string,int>> client_transaction__account__pair;
                client_transaction__account__pair.first = client_id;
                client_transaction__account__pair.second.insert(account_amounts_pair);
                this->client_transaction__account_amounts.insert(client_transaction__account__pair);
            }
            else{
                if(!(this->client_transaction__account_amounts[client_id].count(server_account)>0)){
                    pair<string,int> account_amounts_pair(server_account,deposit_amount);
                    this->client_transaction__account_amounts[client_id].insert(account_amounts_pair);
                }
                else{
                    this->client_transaction__account_amounts[client_id][server_account] += deposit_amount;
                }
            }

            if(this->account_balance.count(server_account)>0 && deposit_amount>0){
                this->account_balance.at(server_account).increase(deposit_amount,client_id);
            }
            else{ // an account is automatically created if it does not exist.
                // insert
                this->account_balance.emplace(server_account, deposit_amount);
            }
            return true;
        }

        void print_balance() {
            for (auto &acc_bal_pair: this->account_balance) {
                /*
                 * Every time a server commits any updates to its objects, it should print the balance of all accounts with non-zero values.
                 */
                if (acc_bal_pair.second.check_positive()) {
                    cout << acc_bal_pair.first << " = " << acc_bal_pair.second.getAmount() << endl;

                }
            }
        }

        bool getBalanceAmount(string server_account, string client_id, int& bal){
            if(this->account_balance.count(server_account)>0){
                bal = this->account_balance[server_account].getAmount(client_id);
                DEBUG_INFO(to_string(bal));
                return true;
            }
            else{
                return false;
            }
        }

        bool withdraw(string server_account, int withdraw_amount, string client_id){
            // current transaction records
            if(!(this->client_transaction__account_amounts.count(client_id)>0)){
                pair<string,int> account_amounts_pair(server_account,-withdraw_amount);
                pair<string, map<string,int>> client_transaction__account__pair;
                client_transaction__account__pair.first = client_id;
                client_transaction__account__pair.second.insert(account_amounts_pair);
                this->client_transaction__account_amounts.insert(client_transaction__account__pair);
            }
            else{
                if(!(this->client_transaction__account_amounts[client_id].count(server_account)>0)){
                    pair<string,int> account_amounts_pair(server_account,-withdraw_amount);
                    this->client_transaction__account_amounts[client_id].insert(account_amounts_pair);
                }
                else{
                    this->client_transaction__account_amounts[client_id][server_account] -= withdraw_amount;

                }
            }

            if(this->account_balance.count(server_account)>0) {
                // The account balance should decrease by the withdrawn amount.
                this->account_balance[server_account].decrease(withdraw_amount,client_id);
            }
            else{
                // reply to the client
                return false;
            }
            return true;
        }

        void commit(string client_id){
            // 2 phase lock requires to release lock related to the transaction (client_id) at this point
            // release the lock and proceed
            if(this->client_transaction__account_amounts.count(client_id)>0){
                this->update_account_list();
                for(auto acc_amt_pair:this->client_transaction__account_amounts[client_id]){
                    auto &bal = this->account_balance[acc_amt_pair.first];
                    bal.release_locks();
                }
                this->client_transaction__account_amounts.erase(client_id); // this transaction of client_id is finished
            }
        }

        void abort(string client_id){
            // All updates made during the transaction must be rolled back.
            if(this->client_transaction__account_amounts.count(client_id)>0){
                for(auto acc_amt_pair:this->client_transaction__account_amounts[client_id]){
                    if(!(count(this->account_permanent.begin(), this->account_permanent.end(), acc_amt_pair.first)>0)){
                        this->account_balance.erase(acc_amt_pair.first);
                    }
                    else{
                        this->account_balance[acc_amt_pair.first].roll_back(acc_amt_pair.second,client_id);
                    }
                }
            }
            else{
                DEBUG_INFO("Nothing to roll back");
            }

            // 2 phase lock requires to release lock related to the transaction (client_id) at this point
            if(this->client_transaction__account_amounts.count(client_id)>0){
                for(auto acc_amt_pair:this->client_transaction__account_amounts[client_id]){
                    auto &bal = this->account_balance[acc_amt_pair.first];
                    bal.release_locks();
                }
                this->client_transaction__account_amounts.erase(client_id); // this transaction of client_id is finished
            }
            else{
                DEBUG_INFO("Nothing to roll back");
            }
        }
};


message_base::MessageBaseServer server;
string server_id;
Transactions transactions;

void server_client_rpc_handling_server(deque<json>* client_rpc_command_queue, mutex* client_rpc_queue_mtx){
    while(true){
        sleep(0.001);
        if(!client_rpc_command_queue->empty()){
            client_rpc_queue_mtx->lock();
            json rpc = client_rpc_command_queue->front();
            client_rpc_queue_mtx->unlock();

            json rpl_rpc;
            if(rpc["type"].get<string>()==message_base::DEPOSIT){
                DEBUG_INFO(message_base::DEPOSIT+"!");
                bool state = transactions.deposit(rpc["account"].get<string>(),rpc["amount"].get<int>(),rpc["clientID"].get<string>());
                DEBUG_INFO(message_base::DEPOSIT+"!");
                // always true
                rpl_rpc = json{{"serverID", server_id},
                               {"state", state},};
                server.unicast(rpc["clientID"].get<string>(),rpl_rpc);
            }
            else if(rpc["type"].get<string>()==message_base::BALANCE){
                DEBUG_INFO(message_base::BALANCE+"!");
                int bal_am = 0;
                if(transactions.getBalanceAmount(rpc["account"].get<string>(),rpc["clientID"].get<string>(),bal_am)){
                    rpl_rpc = json{{"serverID", server_id},
                                   {"state", true},
                                   {"balance", bal_am},};
                }
                else{
                    rpl_rpc = json{{"serverID", server_id},
                                   {"state", false},
                                   {"balance", bal_am},};
                }
                DEBUG_INFO(message_base::BALANCE+"!");
                server.unicast(rpc["clientID"].get<string>(), rpl_rpc);
            }
            else if(rpc["type"].get<string>()==message_base::WITHDRAW){
                DEBUG_INFO(message_base::WITHDRAW+"!");
                if(transactions.withdraw(rpc["account"].get<string>(),rpc["amount"].get<int>(),rpc["clientID"].get<string>())){
                    rpl_rpc = json{{"serverID", server_id},
                                   {"state", true}};
                }else{
                    rpl_rpc = json{{"serverID", server_id},
                                   {"state", false}};
                }
                DEBUG_INFO(message_base::WITHDRAW+"!");
                server.unicast(rpc["clientID"].get<string>(), rpl_rpc);
            }
            else if (rpc["type"].get<string>()==message_base::COMMIT){
                DEBUG_INFO(message_base::COMMIT+"!");
                // 2PC
                if(rpc["CP_NUM"].get<int>()==1){
                    bool state = transactions.check();
                    DEBUG_INFO(message_base::COMMIT+"!");
                    rpl_rpc = json{{"serverID", server_id},
                                   {"state", state}};
                    server.unicast(rpc["clientID"].get<string>(), rpl_rpc);
                }
                else if(rpc["CP_NUM"].get<int>()==2){
                    if(rpc["CP_STATE"].get<bool>()){
                        transactions.commit(rpc["clientID"].get<string>());
                    }else{
                        transactions.abort(rpc["clientID"].get<string>());
                    }
                    DEBUG_INFO(message_base::COMMIT+"!");
                    // no_rpl
                    transactions.print_balance();
                }
            }

            client_rpc_queue_mtx->lock();
            client_rpc_command_queue->pop_front();
            client_rpc_queue_mtx->unlock();
        }
    }
}
void server_recv_worker(message_base::NodeConnection* nc){
    int numbytes;
    char buffer[message_base::BUFFER_SIZE];
    deque<json> client_rpc_command_queue;
    mutex client_rpc_queue_mtx;
    // Dynamic Parallism
    thread client_rpc_handling_thread(server_client_rpc_handling_server, &client_rpc_command_queue,&client_rpc_queue_mtx);
    auto client_rpc_handling_thread_handle = client_rpc_handling_thread.native_handle();
    client_rpc_handling_thread.detach();

    while((numbytes = ::recv(nc->send_recv_socket_fd, buffer, message_base::BUFFER_SIZE, 0))>0)
    {
        buffer[numbytes] = '\0';
        string j_str = buffer;
        json rpc = json::parse(j_str);
        string client_id = rpc["clientID"].get<string>();

        if (nc->node_identifier == "node"){ // not initialized its identifier
            nc->node_identifier = client_id;
        }

        json rpl_rpc;
        // tell if this is ABORT
        if (rpc["type"].get<string>()==message_base::ABORT){
            DEBUG_INFO(message_base::ABORT+"!");
            rpl_rpc = json{{"serverID", server_id},
                           {"state", true}};
            transactions.abort(rpc["clientID"].get<string>());
            client_rpc_queue_mtx.unlock();
            pthread_cancel(client_rpc_handling_thread_handle); // cancel the current thread
            server.unicast(rpc["clientID"].get<string>(), rpl_rpc);

            // clear the queue of previous transaction
            client_rpc_queue_mtx.lock();
            client_rpc_command_queue.clear();
            client_rpc_queue_mtx.unlock();

            client_rpc_handling_thread = thread(server_client_rpc_handling_server, &client_rpc_command_queue,&client_rpc_queue_mtx);
            client_rpc_handling_thread_handle = client_rpc_handling_thread.native_handle();
            client_rpc_handling_thread.detach(); // detach again
            DEBUG_INFO("Relaunch thread successful!");
        }
        else{// if not ABORT, then put in queue
            client_rpc_queue_mtx.lock();
            client_rpc_command_queue.push_back(rpc);
            client_rpc_queue_mtx.unlock();
        }
    }
    //::close(nc->send_recv_socket_fd);
}

// server
int main(int argc, char const *argv[]) {
    string config_file;
    vector<message_base::ServerInfo> sinfo;
    if(argc==3){
        server_id = argv[1];
        config_file = argv[2];

        ifstream configfilestream(config_file);

        string node_identifier, node_address;
        int port_no;
        // Read the node config file and prepare the required data structures related to the nodes
        if (configfilestream.is_open()) {
            cout << "config file open successful" << endl;
            for (int i = 0; i < 5; ++i) {
                message_base::ServerInfo temp_sinfo;
                configfilestream >> node_identifier >> node_address >> port_no;
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

    DEBUG_INFO("Waiting for connections");
    server = message_base::MessageBaseServer(server_id,sinfo);
    server.server_start(server_recv_worker);
}