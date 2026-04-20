#include "dbms/grpc/sql_service_impl.h"

namespace dbms
{
namespace grpc_api
{

grpc::Status SqlServiceImpl::OpenSession(grpc::ServerContext* context,
                                         const dbms::rpc::OpenSessionRequest* request,
                                         dbms::rpc::OpenSessionResponse* response)
{
    (void)context;
    (void)request;

    std::lock_guard<std::mutex> lock(_mutex);
    const std::string session_id = std::to_string(_next_session_id++);
    _sessions.emplace(session_id, std::make_unique<SqlApi>(_engine));

    response->set_ok(true);
    response->set_session_id(session_id);
    response->clear_error();
    return grpc::Status::OK;
}

grpc::Status SqlServiceImpl::Execute(grpc::ServerContext* context,
                                     const dbms::rpc::ExecuteRequest* request,
                                     dbms::rpc::ExecuteResponse* response)
{
    (void)context;

    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _sessions.find(request->session_id());
    if (it == _sessions.end()) {
        response->set_ok(false);
        response->set_is_select(false);
        response->clear_json();
        response->set_error("Unknown session");
        return grpc::Status::OK;
    }

    const SqlResponse result = it->second->execute_sql(request->sql());
    response->set_ok(result.ok);
    response->set_is_select(result.is_select);
    response->set_json(result.json);
    response->set_error(result.error);
    return grpc::Status::OK;
}

grpc::Status SqlServiceImpl::CloseSession(grpc::ServerContext* context,
                                          const dbms::rpc::CloseSessionRequest* request,
                                          dbms::rpc::CloseSessionResponse* response)
{
    (void)context;

    std::lock_guard<std::mutex> lock(_mutex);
    const auto erased = _sessions.erase(request->session_id());
    if (erased == 0u) {
        response->set_ok(false);
        response->set_error("Unknown session");
        return grpc::Status::OK;
    }

    response->set_ok(true);
    response->clear_error();
    return grpc::Status::OK;
}

}
}
