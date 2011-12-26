namespace py discovery

/* Simple exception type */
exception Exception
{
    1: string msg
}

exception NotFound
{
    1: string msg
}

struct Service {
    1: string name,
    2: string host,
    3: i32 port
}

service Discovery {

    bool register_service (1: Service service_details)
    throws (1: Exception ex);

    bool remove_service (1: Service service_details)
    throws (1: Exception ex);
    
    Service find_service (1: string service_name)
    throws (1: NotFound ex);
}
