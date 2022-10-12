<%
    sysdataset_path = middleware.call_sync('systemdataset.config')['path']
    if not sysdataset_path:
        middleware.logger.error("glusterd.conf: system dataset is not mounted")
        raise FileShouldNotExist()

    work_dir = sysdataset_path + '/glusterd'
%>\

volume management
    type mgmt/glusterd
    option working-directory ${work_dir}
    option transport-type socket
    option transport.socket.keepalive-time 10
    option transport.socket.keepalive-interval 2
    option transport.socket.read-fail-log off
    option transport.socket.listen-port 24007
    option ping-timeout 0
    option event-threads 1
#   option lock-timer 180
#   option transport.address-family inet6
#   option base-port 49152
    option max-port  60999
end-volume