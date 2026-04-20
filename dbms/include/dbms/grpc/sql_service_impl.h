#ifndef COURSEWORK_DBMS_GRPC_SQL_SERVICE_IMPL_H
#define COURSEWORK_DBMS_GRPC_SQL_SERVICE_IMPL_H

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "dbms/core/dbms.h"
#include "dbms/sql/sql_api.h"
#include "sql_service.grpc.pb.h"

namespace dbms
{
namespace grpc_api
{

class SqlServiceImpl final : public dbms::rpc::SqlService::Service
{
public:
    explicit SqlServiceImpl(std::filesystem::path data_root = Dbms::default_data_root())
        : _engine(std::move(data_root))
    {
    }

    grpc::Status OpenSession(grpc::ServerContext* context,
                             const dbms::rpc::OpenSessionRequest* request,
                             dbms::rpc::OpenSessionResponse* response) override;

    grpc::Status Execute(grpc::ServerContext* context,
                         const dbms::rpc::ExecuteRequest* request,
                         dbms::rpc::ExecuteResponse* response) override;

    grpc::Status CloseSession(grpc::ServerContext* context,
                              const dbms::rpc::CloseSessionRequest* request,
                              dbms::rpc::CloseSessionResponse* response) override;

private:
    Dbms _engine;
    std::mutex _mutex;
    std::uint64_t _next_session_id = 1;
    std::unordered_map<std::string, std::unique_ptr<SqlApi>> _sessions;
};

}
}

#endif
