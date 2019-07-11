FROM scratch

COPY s3 /

VOLUME /data

CMD ["/s3", "/data"]