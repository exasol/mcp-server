MCP Server Deployment
=====================

The MCP server can be deployed either locally, as a Python package, or as a remote HTTP
server. This section of the guide explains the details of how to run the MCP server as
an HTTP server.

Direct Python Deployment
------------------------

Install the Exasol MCP Server Python package. Below are examples of `pip` and `uv` commands.

.. code-block:: shell

   pip install exasol-mcp-server

.. code-block:: shell

   uv tool install exasol-mcp-server@latest

Set the required environment variables and, optionally, the settings json file as described
in the :doc:`server_setup` and other guides referenced there.

Start the HTTP server from the command line, as in the example below.

.. code-block:: shell

    exasol-mcp-server-http --port <server-port>

Docker Deployment
-----------------

When running the MCP Server from a Docker container, the environment variables and the settings
file shall be passed as the arguments to the `docker run` command, as in the example below.

Create a file `settings.json` and save it somewhere in the host machine.

.. code-block:: json

    {
        "enable_read_query": true
    }

Now create and run the Docker container.

.. code-block:: shell

    docker run \
        -p 7766:4896 \
        -e EXA_DSN=my_dsn \
        -e EXA_USER=my_user-name \
        -e EXA_PASSWORD=my-password \
        -e FASTMCP_SERVER_AUTH=exa.fastmcp.server.auth.oauth_proxy.OAuthProxy \
        -e EXA_AUTH_UPSTREAM_AUTHORIZATION_ENDPOINT=my_identity_provider/oauth2/authorize \
        -e EXA_AUTH_UPSTREAM_TOKEN_ENDPOINT=my_identity_provider/oauth2/token \
        -e EXA_AUTH_UPSTREAM_CLIENT_ID=my_client_id \
        -e EXA_AUTH_UPSTREAM_CLIENT_SECRET=my_client_secret \
        -e EXA_AUTH_JWKS_URI=my_identity_provider/jwks \
        -e EXA_AUTH_BASE_URL=my_mcp_server \
        -e EXA_MCP_SETTINGS=/app/settings.json \
        -v local_path_to_settings.json:/app/settings.json \
        exadockerci4/exasol-mcp-server:latest
        --port 4896

In this example, the server is configured to use the generic OAuth Proxy provider.
The server starts at the Docker container's port 4896, which is published to the
host port 7766. Normally, the base URL of the MCP server will be a secure address
of the server, e.g. "https://the_server_address", as described in the next section.
For example, if the server is deployed in an AWS EC2 instance, the EXA_AUTH_BASE_URL
can be set to "https://ec2-xx-xxx-xx-xx.my-region.compute.amazonaws.com".

Note that the path to the settings.json file must be an absolute path.

Reverse proxy configuration
---------------------------

Exasol MCP Server runs as an HTTP server. Connecting to the server via an unsecured
HTTP channel should only be considered as a way of testing it. A production deployment
would normally include a reverse proxy, allowing a secure connection to the server
from anywhere in the internet. Below is an example of a configuration for Nginx
that is commonly used as a reverse proxy.

Check if the Nginx is installed. The following command should print its version.

.. code-block:: shell

   nginx -v

Create the Nginx configuration file for Exasol MCP Server. This configuration relates
to the previous example of running the server in a Docker container. In particular,
it assumes that the server is accessible at the local port 7766.

.. code-block:: C

    server {
        listen 80;
        # Public name or address of the server
        server_name ec2-xx-xxx-xx-xx.my-region.compute.amazonaws.com;

        # Redirect all HTTP to HTTPS
        return 301 https://$host$request_uri;
    }

    server {
        listen 443 ssl;
        # Public name or address of the server
        server_name ec2-xx-xxx-xx-xx.my-region.compute.amazonaws.com;

        # SSL certificate
        ssl_certificate /etc/nginx/ssl/my_certificate.pem;
        ssl_certificate_key /etc/nginx/ssl/my_certificate_key.pem;

        # SSL settings
        ssl_protocols TLSv1.2 TLSv1.3;
        ssl_ciphers HIGH:!aNULL:!MD5;

        location / {
            # Local address where the server is accessible from the host
            proxy_pass http://localhost:7766;

            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }

Point to the created file in the Nginx own configuration file - `nginx.conf`.
The location of this file in a standard installation of Nginx depends on the OS::

    * Linux: /etc/nginx/nginx.conf
    * macOS: /usr/local/etc/nginx/nginx.conf
    * Windows: C:\nginx\conf\nginx.conf

In the `http` section, add the following line.

.. code-block:: C

    http {
        ...
        include <path_to_the_server_config_file>;
        ...
    }

Validate the new configuration by running the following command. Administrator privileges
might by required.

.. code-block:: shell

   nginx -t

Let Nginx use the new configuration.

.. code-block:: shell

    nginx -s reload
