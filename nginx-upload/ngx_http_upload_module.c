/*
 * Copyright 2009-2010 Michael Dirolf
 *
 * Dual Licensed under the Apache License, Version 2.0 and the GNU
 * General Public License, version 2 or (at your option) any later
 * version.
 *
 * -- Apache License
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * -- GNU GPL
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
/*
 * TODO range support http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "mongo-c-driver/src/mongo.h"
#include "mongo-c-driver/src/gridfs.h"
#include <signal.h>
#include <stdio.h>

#define MULTIPART_FORM_DATA_STRING      "multipart/form-data"
#define BOUNDARY_STRING                 "boundary="
#define CONTENT_DISPOSITION_STRING      "Content-Disposition:"
#define CONTENT_TYPE_STRING             "Content-Type:"
#define FORM_DATA_STRING                "form-data"
#define ATTACHMENT_STRING               "attachment"
#define FILENAME_STRING                 "filename=\""
#define FIELDNAME_STRING                "name=\""

#define NGX_UPLOAD_MALFORMED    -1
#define NGX_UPLOAD_NOMEM        -2
#define NGX_UPLOAD_IOERROR      -3
#define NGX_UPLOAD_SCRIPTERROR  -4

#define MONGO_MAX_RETRIES_PER_REQUEST 1
#define MONGO_RECONNECT_WAITTIME 500 //ms
#define TRUE 1
#define FALSE 0

/*
 * State of multipart/form-data parser
 */
typedef enum {
	upload_state_boundary_seek,
	upload_state_after_boundary,
	upload_state_headers,
	upload_state_data,
	upload_state_finish
} upload_state_t;

typedef struct {
	ngx_table_elt_t          value;
	ngx_array_t             *field_lengths;
	ngx_array_t             *field_values;
	ngx_array_t             *value_lengths;
	ngx_array_t             *value_values;
} ngx_http_upload_field_template_t;

/*
 * Upload configuration for specific location
 */

/* Parse config directive */
static char * ngx_http_mongo(ngx_conf_t *cf, ngx_command_t *cmd, void *dummy);
/* Parse config directive */
static char* ngx_http_gridfs(ngx_conf_t* directive, ngx_command_t* command, void* gridfs_conf);

static void* ngx_http_gridfs_create_main_conf(ngx_conf_t* directive);
static void* ngx_http_gridfs_create_loc_conf(ngx_conf_t* directive);
static char* ngx_http_gridfs_merge_loc_conf(ngx_conf_t* directive, void* parent, void* child);
static ngx_int_t ngx_http_gridfs_init_worker(ngx_cycle_t* cycle);
static ngx_int_t ngx_http_gridfs_handler(ngx_http_request_t* request);
static void ngx_http_gridfs_cleanup(void* data);

typedef struct {
    ngx_str_t db;
    ngx_str_t root_collection;
    ngx_str_t field;
    ngx_uint_t type;
    ngx_str_t user;
    ngx_str_t pass;
    ngx_str_t mongo;
    ngx_array_t* mongods; /* ngx_http_mongod_server_t */
    ngx_str_t replset; /* Name of the replica set, if connecting. */
	ngx_path_t        *store_path;
	ngx_uint_t         store_access;
	size_t             buffer_size;
	size_t             max_header_len;
	ngx_array_t       *field_templates;
} ngx_http_gridfs_loc_conf_t;

typedef struct {
    ngx_str_t db;
    ngx_str_t user;
    ngx_str_t pass;
} ngx_http_mongo_auth_t;

typedef struct {
    ngx_str_t name;
    mongo_connection conn;
	gridfs gfs;
    ngx_array_t *auths; /* ngx_http_mongo_auth_t */
} ngx_http_mongo_connection_t;

/* Maybe we should store a list of addresses instead. */
typedef struct {
    ngx_str_t host;
    in_port_t port;
} ngx_http_mongod_server_t;

typedef struct {
    ngx_array_t loc_confs; /* ngx_http_gridfs_loc_conf_t */
} ngx_http_gridfs_main_conf_t;

typedef struct {
    mongo_cursor ** cursors;
    ngx_uint_t numchunks;
} ngx_http_gridfs_cleanup_t;

/*
 * Upload module context
 */
typedef struct ngx_http_upload_ctx_s {
	ngx_str_t            boundary;
	u_char              *boundary_start;
	u_char              *boundary_pos;

	upload_state_t		 state;

	u_char              *header_accumulator;
	u_char              *header_accumulator_end;
	u_char              *header_accumulator_pos;

	ngx_str_t            field_name;
	ngx_str_t            file_name;
	ngx_str_t            content_type;

	u_char              *output_buffer;
	u_char              *output_buffer_end;
	u_char              *output_buffer_pos;

	ngx_pool_t          *pool;

	ngx_int_t (*start_part_f)(struct ngx_http_upload_ctx_s *upload_ctx);
	void (*finish_part_f)(struct ngx_http_upload_ctx_s *upload_ctx);
	void (*abort_part_f)(struct ngx_http_upload_ctx_s *upload_ctx);
	ngx_int_t (*flush_buffer_f)(struct ngx_http_upload_ctx_s *upload_ctx,
		u_char *buf, size_t len);

	ngx_http_request_t  *request;
	ngx_log_t           *log;

	gridfile             grid_file;
	ngx_file_t           output_file;
	ngx_chain_t         *chain;
	ngx_chain_t         *last;
	ngx_chain_t         *checkpoint;
	ngx_chain_t         *media, *media_last;

	unsigned int         first_part:1;
	unsigned int         discard_data:1;
	unsigned int         is_file:1;
} ngx_http_gridfs_ctx_t;

static ngx_int_t ngx_http_upload_handler(ngx_http_request_t *r);
static void ngx_http_upload_response_handler(ngx_http_request_t *r);

static ngx_int_t ngx_http_upload_add_variables(ngx_conf_t *cf);
static ngx_int_t ngx_http_upload_variable(ngx_http_request_t *r,
	ngx_http_variable_value_t *v,  uintptr_t data);

static ngx_int_t ngx_http_upload_start_handler(ngx_http_gridfs_ctx_t *u);
static void ngx_http_upload_finish_handler(ngx_http_gridfs_ctx_t *u);
static void ngx_http_upload_abort_handler(ngx_http_gridfs_ctx_t *u);
static ngx_int_t ngx_http_upload_flush_buffer(ngx_http_gridfs_ctx_t *ctx,
	u_char *buf, size_t len);

static ngx_int_t ngx_http_upload_append_field(ngx_http_gridfs_ctx_t *ctx,
	ngx_str_t *name, ngx_str_t *value);

static ngx_int_t ngx_http_upload_read_client_request_body(ngx_http_request_t *r);
static void ngx_http_read_upload_client_request_body_handler(ngx_http_request_t *r);
static ngx_int_t ngx_http_upload_do_read_client_request_body(ngx_http_request_t *r);
static ngx_int_t ngx_http_upload_process_client_request_body(ngx_http_request_t *r);

static char *ngx_http_upload_set_form_field(ngx_conf_t *cf,
	ngx_command_t *cmd, void *conf);

static ngx_int_t ngx_http_gridfs_start_handler(ngx_http_gridfs_ctx_t *u);
static void ngx_http_gridfs_finish_handler(ngx_http_gridfs_ctx_t *u);
static void ngx_http_gridfs_abort_handler(ngx_http_gridfs_ctx_t *u);
static ngx_int_t ngx_http_gridfs_flush_buffer(ngx_http_gridfs_ctx_t *ctx,
	u_char *buf, size_t len);

/*
 * upload_initialize_ctx
 *
 * Initialize upload context. Memory for upload context which is being passed
 * as upload_ctx parameter could be allocated anywhere and should not be freed
 * prior to upload_shutdown_ctx call.
 *
 * IMPORTANT:
 * 
 * After initialization the following routine SHOULD BE called:
 * 
 * upload_parse_content_type -- to assign part boundary 
 *
 * Parameter:
 *     ctx -- upload context which is being initialized
 * 
 */
static void upload_initialize_ctx(ngx_http_gridfs_ctx_t *ctx);

/*
 * upload_shutdown_ctx
 *
 * Shutdown upload context. Discard all remaining data and 
 * free all memory associated with upload context.
 *
 * Parameter:
 *     ctx -- upload context which is being shut down
 * 
 */
static void upload_shutdown_ctx(ngx_http_gridfs_ctx_t *ctx);

/*
 * upload_prepare_part
 *
 * Starts multipart stream processing. Initializes internal buffers
 * and pointers
 *
 * Parameter:
 *     ctx -- upload context which is being initialized
 * 
 * Return value:
 *               NGX_OK on success
 *               NGX_ERROR if error has occured
 *
 */
static ngx_int_t upload_prepare_part(ngx_http_gridfs_ctx_t *ctx,
	ngx_http_gridfs_loc_conf_t  *ulcf);

/*
 * upload_parse_content_type
 *
 * Parse and verify content type from HTTP header, extract boundary and
 * assign it to upload context
 * 
 * Parameters:
 *     ctx -- upload context to populate
 *     content_type -- value of Content-Type header to parse
 *
 * Return value:
 *     NGX_OK on success
 *     NGX_ERROR if error has occured
 */
static ngx_int_t upload_parse_content_type(ngx_http_gridfs_ctx_t *ctx,
	ngx_str_t *content_type);

/*
 * upload_process_buf
 *
 * Process buffer with multipart stream starting from start and terminating
 * by end, operating on upload_ctx. The header information is accumulated in
 * upload_ctx and could be retrieved using upload_get_file_content_type,
 * upload_get_file_name, upload_get_field_name functions. This call can issue
 * one or more calls to start_upload_file, finish_upload_file,
 * abort_upload_file and flush_buffer routines.
 *
 * Returns value > 0 if context is ready to process next portion of data,
 *               = 0 if processing finished and remaining data could be
 *                    discarded,
 *                -1 stream is malformed
 *                -2 insufficient memory 
 */
static ngx_int_t upload_process_buf(ngx_http_gridfs_ctx_t *ctx,
	u_char *start, u_char *end);

static ngx_path_init_t        ngx_http_upload_temp_path = {
	ngx_string(NGX_HTTP_PROXY_TEMP_PATH), { 1, 2, 0 }
};

/* Array specifying how to handle configuration directives. */
static ngx_command_t ngx_http_gridfs_commands[] = {

    {
        ngx_string("upload_mongo"),
        NGX_HTTP_LOC_CONF | NGX_CONF_1MORE,
        ngx_http_mongo,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },

    {
        ngx_string("upload_gridfs"),
        NGX_HTTP_LOC_CONF | NGX_CONF_1MORE,
        ngx_http_gridfs,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },

	/*
	 * Specifies base path of file store
	 */
	{ ngx_string("upload_store"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	ngx_conf_set_path_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_gridfs_loc_conf_t, store_path),
	NULL },

	/*
	 * Specifies the access mode for files in store
	 */
	{ ngx_string("upload_store_access"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	ngx_conf_set_access_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_gridfs_loc_conf_t, store_access),
	NULL },

	/*
	 * Specifies the size of buffer, which will be used
	 * to write data to disk
	 */
	{ ngx_string("upload_buffer_size"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	ngx_conf_set_size_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_gridfs_loc_conf_t, buffer_size),
	NULL },

	/*
 	 * Specifies the maximal length of the part header
 	 */
	{ ngx_string("upload_max_part_header_len"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	ngx_conf_set_size_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_gridfs_loc_conf_t, max_header_len),
	NULL },

	/*
	 * Specifies the field to set in altered response body
	 */
	{ ngx_string("upload_set_form_field"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF
	|NGX_CONF_TAKE2,
	ngx_http_upload_set_form_field,
	NGX_HTTP_LOC_CONF_OFFSET,
	0,
	NULL},

    ngx_null_command
};

/* Module context. */
static ngx_http_module_t ngx_http_upload_module_ctx = {
	ngx_http_upload_add_variables,         /* preconfiguration */
	NULL, /* postconfiguration */
    ngx_http_gridfs_create_main_conf,
    NULL, /* init main configuration */
    NULL, /* create server configuration */
    NULL, /* init serever configuration */
    ngx_http_gridfs_create_loc_conf,
    ngx_http_gridfs_merge_loc_conf
};

/* Module definition. */
ngx_module_t ngx_http_upload_module = {
    NGX_MODULE_V1,
    &ngx_http_upload_module_ctx,
    ngx_http_gridfs_commands,
    NGX_HTTP_MODULE,
    NULL,
    NULL,
    ngx_http_gridfs_init_worker,
    NULL,
    NULL,
    NULL,
    NULL,
    NGX_MODULE_V1_PADDING
};

static ngx_str_t  ngx_http_upload_field_name = ngx_string("upload_field_name");
static ngx_str_t  ngx_http_upload_content_type = ngx_string("upload_content_type");
static ngx_str_t  ngx_http_upload_file_name = ngx_string("upload_file_name");
static ngx_str_t  ngx_http_upload_tmp_path = ngx_string("upload_tmp_path");

static ngx_str_t  ngx_http_upload_empty_field_value = ngx_null_string;

ngx_array_t ngx_http_mongo_connections;

/* Parse the 'mongo' directive. */
static char * ngx_http_mongo(ngx_conf_t *cf, ngx_command_t *cmd, void *void_conf) {
    ngx_str_t *value;
    ngx_url_t u;
    ngx_uint_t i;
    ngx_uint_t start;
    ngx_http_mongod_server_t *mongod_server;
    ngx_http_gridfs_loc_conf_t *gridfs_loc_conf;

    gridfs_loc_conf = void_conf;

    value = cf->args->elts;
    gridfs_loc_conf->mongo = value[1];
    gridfs_loc_conf->mongods = ngx_array_create(cf->pool, 7,
                                                sizeof(ngx_http_mongod_server_t));
    if (gridfs_loc_conf->mongods == NULL) {
        return NULL;
    }

    /* If nelts is greater than 3, then the user has specified more than one
     * setting in the 'mongo' directive. So we assume that we're connecting
     * to a replica set and that the first string of the directive is the replica
     * set name. We also start looking for host-port pairs at position 2; otherwise,
     * we start at position 1.
     */
    if( cf->args->nelts >= 3 ) {
        gridfs_loc_conf->replset.len = strlen( (char *)(value + 1)->data );
        gridfs_loc_conf->replset.data = ngx_pstrdup( cf->pool, value + 1 );
        start = 2;
    } else
        start = 1;

    for (i = start; i < cf->args->nelts; i++) {

        ngx_memzero(&u, sizeof(ngx_url_t));

        u.url = value[i];
        u.default_port = 27017;

        if (ngx_parse_url(cf->pool, &u) != NGX_OK) {
            if (u.err) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "%s in mongo \"%V\"", u.err, &u.url);
            }
            return NGX_CONF_ERROR;
        }

        mongod_server = ngx_array_push(gridfs_loc_conf->mongods);
        mongod_server->host = u.host;
        mongod_server->port = u.port;

    }

    return NGX_CONF_OK;
}

/* Parse the 'gridfs' directive. */
static char* ngx_http_gridfs(ngx_conf_t* cf, ngx_command_t* command, void* void_conf) {
    ngx_http_gridfs_loc_conf_t *gridfs_loc_conf = void_conf;
    ngx_http_core_loc_conf_t* core_conf;
    ngx_str_t *value, type;
    volatile ngx_uint_t i;

    core_conf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    core_conf-> handler = ngx_http_gridfs_handler;

    value = cf->args->elts;
    gridfs_loc_conf->db = value[1];

    /* Parse the parameters */
    for (i = 2; i < cf->args->nelts; i++) {
        if (ngx_strncmp(value[i].data, "root_collection=", 16) == 0) { 
            gridfs_loc_conf->root_collection.data = (u_char *) &value[i].data[16];
            gridfs_loc_conf->root_collection.len = ngx_strlen(&value[i].data[16]);
            continue;
        }

        if (ngx_strncmp(value[i].data, "field=", 6) == 0) {
            gridfs_loc_conf->field.data = (u_char *) &value[i].data[6];
            gridfs_loc_conf->field.len = ngx_strlen(&value[i].data[6]);

            /* Currently only support for "_id" and "filename" */
            if (gridfs_loc_conf->field.data != NULL
                && ngx_strcmp(gridfs_loc_conf->field.data, "filename") != 0
                && ngx_strcmp(gridfs_loc_conf->field.data, "_id") != 0) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "Unsupported Field: %s", gridfs_loc_conf->field.data);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "type=", 5) == 0) { 
            type = (ngx_str_t) ngx_string(&value[i].data[5]);

            /* Currently only support for "objectid", "string", and "int" */
            if (type.len == 0) {
                gridfs_loc_conf->type = NGX_CONF_UNSET_UINT;
            } else if (ngx_strcasecmp(type.data, (u_char *)"objectid") == 0) {
                gridfs_loc_conf->type = bson_oid;
            } else if (ngx_strcasecmp(type.data, (u_char *)"string") == 0) {
                gridfs_loc_conf->type = bson_string;
            } else if (ngx_strcasecmp(type.data, (u_char *)"int") == 0) {
                gridfs_loc_conf->type = bson_int;
            } else {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "Unsupported Type: %s", (char *)value[i].data);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "user=", 5) == 0) { 
            gridfs_loc_conf->user.data = (u_char *) &value[i].data[5];
            gridfs_loc_conf->user.len = ngx_strlen(&value[i].data[5]);
            continue;
        }

        if (ngx_strncmp(value[i].data, "pass=", 5) == 0) {
            gridfs_loc_conf->pass.data = (u_char *) &value[i].data[5];
            gridfs_loc_conf->pass.len = ngx_strlen(&value[i].data[5]);
            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    if (gridfs_loc_conf->field.data != NULL
        && ngx_strcmp(gridfs_loc_conf->field.data, "filename") == 0
        && gridfs_loc_conf->type != bson_string) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "Field: filename, must be of Type: string");
        return NGX_CONF_ERROR;
    }

    if ((gridfs_loc_conf->user.data == NULL || gridfs_loc_conf->user.len == 0)
        && !(gridfs_loc_conf->pass.data == NULL || gridfs_loc_conf->pass.len == 0)) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "Password without username");
        return NGX_CONF_ERROR;
    }

    if (!(gridfs_loc_conf->user.data == NULL || gridfs_loc_conf->user.len == 0)
        && (gridfs_loc_conf->pass.data == NULL || gridfs_loc_conf->pass.len == 0)) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "Username without password");
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}

static void *ngx_http_gridfs_create_main_conf(ngx_conf_t *cf) {
    ngx_http_gridfs_main_conf_t  *gridfs_main_conf;

    gridfs_main_conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_gridfs_main_conf_t));
    if (gridfs_main_conf == NULL) {
        return NULL;
    }

    if (ngx_array_init(&gridfs_main_conf->loc_confs, cf->pool, 4,
                       sizeof(ngx_http_gridfs_loc_conf_t *))
        != NGX_OK) {
        return NULL;
    }

    return gridfs_main_conf;
}

static void* ngx_http_gridfs_create_loc_conf(ngx_conf_t* directive) {
    ngx_http_gridfs_loc_conf_t* gridfs_conf;

    gridfs_conf = ngx_pcalloc(directive->pool, sizeof(ngx_http_gridfs_loc_conf_t));
    if (gridfs_conf == NULL) {
        ngx_conf_log_error(NGX_LOG_EMERG, directive, 0,
                           "Failed to allocate memory for GridFS Location Config.");
        return NGX_CONF_ERROR;
    }

    gridfs_conf->db.data = NULL;
    gridfs_conf->db.len = 0;
    gridfs_conf->root_collection.data = NULL;
    gridfs_conf->root_collection.len = 0;
    gridfs_conf->field.data = NULL;
    gridfs_conf->field.len = 0;
    gridfs_conf->type = NGX_CONF_UNSET_UINT;
    gridfs_conf->user.data = NULL;
    gridfs_conf->user.len = 0;
    gridfs_conf->pass.data = NULL;
    gridfs_conf->pass.len = 0;
    gridfs_conf->mongo.data = NULL;
    gridfs_conf->mongo.len = 0;
    gridfs_conf->mongods = NGX_CONF_UNSET_PTR;

	gridfs_conf->store_access = 0600;
	gridfs_conf->buffer_size = NGX_CONF_UNSET_SIZE;
	gridfs_conf->max_header_len = NGX_CONF_UNSET_SIZE;

    return gridfs_conf;
}

static char* ngx_http_gridfs_merge_loc_conf(ngx_conf_t* cf, void* void_parent, void* void_child) {
    ngx_http_gridfs_loc_conf_t *parent = void_parent;
    ngx_http_gridfs_loc_conf_t *child = void_child;
    ngx_http_gridfs_main_conf_t *gridfs_main_conf = ngx_http_conf_get_module_main_conf(cf, ngx_http_upload_module);
    ngx_http_gridfs_loc_conf_t **gridfs_loc_conf;
    ngx_http_mongod_server_t *mongod_server;

    ngx_conf_merge_str_value(child->db, parent->db, NULL);
    ngx_conf_merge_str_value(child->root_collection, parent->root_collection, "fs");
    ngx_conf_merge_str_value(child->field, parent->field, "_id");
    ngx_conf_merge_uint_value(child->type, parent->type, bson_oid);
    ngx_conf_merge_str_value(child->user, parent->user, NULL);
    ngx_conf_merge_str_value(child->pass, parent->pass, NULL);
    ngx_conf_merge_str_value(child->mongo, parent->mongo, "127.0.0.1:27017");

    if (child->mongods == NGX_CONF_UNSET_PTR) {
        if (parent->mongods != NGX_CONF_UNSET_PTR) {
            child->mongods = parent->mongods;
        } else {
            child->mongods = ngx_array_create(cf->pool, 4,
                                              sizeof(ngx_http_mongod_server_t));
            mongod_server = ngx_array_push(child->mongods);
            mongod_server->host.data = (u_char *)"127.0.0.1";
            mongod_server->host.len = sizeof("127.0.0.1") - 1;
            mongod_server->port = 27017;
        }
    }

    // Add the local gridfs conf to the main gridfs conf
    if (child->db.data) {
        gridfs_loc_conf = ngx_array_push(&gridfs_main_conf->loc_confs);
        *gridfs_loc_conf = child;
    }

	ngx_conf_merge_path_value(cf,
		&child->store_path,
		parent->store_path,
		&ngx_http_upload_temp_path);

	ngx_conf_merge_uint_value(child->store_access,
		parent->store_access, 0600);
	ngx_conf_merge_size_value(child->buffer_size,
		parent->buffer_size, (size_t)ngx_pagesize);
	ngx_conf_merge_size_value(child->max_header_len,
		parent->max_header_len, (size_t)512);
	return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_upload_add_variables(ngx_conf_t *cf) {
	ngx_http_variable_t  *var;

	if (!(var = ngx_http_add_variable(cf, &ngx_http_upload_field_name,
		NGX_HTTP_VAR_CHANGEABLE|NGX_HTTP_VAR_NOCACHEABLE|NGX_HTTP_VAR_NOHASH)))
		return NGX_ERROR;
	var->get_handler = ngx_http_upload_variable;
	var->data = offsetof(ngx_http_gridfs_ctx_t, field_name);

	if (!(var = ngx_http_add_variable(cf, &ngx_http_upload_content_type,
		NGX_HTTP_VAR_CHANGEABLE|NGX_HTTP_VAR_NOCACHEABLE|NGX_HTTP_VAR_NOHASH)))
		return NGX_ERROR;
	var->get_handler = ngx_http_upload_variable;
	var->data = offsetof(ngx_http_gridfs_ctx_t, content_type);

	if (!(var = ngx_http_add_variable(cf, &ngx_http_upload_file_name,
		NGX_HTTP_VAR_CHANGEABLE|NGX_HTTP_VAR_NOCACHEABLE|NGX_HTTP_VAR_NOHASH)))
		return NGX_ERROR;
	var->get_handler = ngx_http_upload_variable;
	var->data = offsetof(ngx_http_gridfs_ctx_t, file_name);

	if (!(var = ngx_http_add_variable(cf, &ngx_http_upload_tmp_path,
		NGX_HTTP_VAR_CHANGEABLE|NGX_HTTP_VAR_NOCACHEABLE|NGX_HTTP_VAR_NOHASH)))
		return NGX_ERROR;
	var->get_handler = ngx_http_upload_variable;
	var->data = offsetof(ngx_http_gridfs_ctx_t, output_file.name);

	return NGX_OK;
}

static ngx_int_t
ngx_http_upload_variable(ngx_http_request_t *r,
	ngx_http_variable_value_t *v, uintptr_t data) {
	ngx_http_gridfs_ctx_t  *ctx;
	ngx_str_t              *value;

	v->valid = 1;
	v->no_cacheable = 0;
	v->not_found = 0;

	ctx = ngx_http_get_module_ctx(r, ngx_http_upload_module);
	value = (ngx_str_t *)((char *)ctx + data);
	v->data = value->data;
	v->len = value->len;

	return NGX_OK;
}

static char *
ngx_http_upload_set_form_field(ngx_conf_t *cf,
	ngx_command_t *cmd, void *conf) {
	ngx_int_t                   n;
	ngx_str_t                  *value;
	ngx_http_script_compile_t   sc;
	ngx_http_upload_field_template_t *h;
	ngx_http_gridfs_loc_conf_t *ulcf = conf;

	value = cf->args->elts;

	if (!(ulcf->field_templates)) {
		if (!(ulcf->field_templates = ngx_array_create(cf->pool, 1,
			sizeof(ngx_http_upload_field_template_t))))
			return NGX_CONF_ERROR;
	}

	if (!(h = ngx_array_push(ulcf->field_templates)))
		return NGX_CONF_ERROR;

	h->value.hash = 1;
	h->value.key = value[1];
	h->value.value = value[2];
	h->field_lengths = NULL;
	h->field_values = NULL;
	h->value_lengths = NULL;
	h->value_values = NULL;

	/* Compile field name */
	if (!(n = ngx_http_script_variables_count(&value[1])))
		return NGX_CONF_OK;
	ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));
	sc.cf = cf;
	sc.source = &value[1];
	sc.lengths = &h->field_lengths;
	sc.values = &h->field_values;
	sc.variables = n;
	sc.complete_lengths = 1;
	sc.complete_values = 1;
	if (NGX_OK != ngx_http_script_compile(&sc))
		return NGX_CONF_ERROR;

	/* Compile field value */
	if (!(n = ngx_http_script_variables_count(&value[2])))
		return NGX_CONF_OK;
	ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));
	sc.cf = cf;
	sc.source = &value[2];
	sc.lengths = &h->value_lengths;
	sc.values = &h->value_values;
	sc.variables = n;
	sc.complete_lengths = 1;
	sc.complete_values = 1;
	if (NGX_OK != ngx_http_script_compile(&sc))
		return NGX_CONF_ERROR;

	return NGX_CONF_OK;
}


static ngx_http_mongo_connection_t* ngx_http_get_mongo_connection( ngx_str_t name ) {
    ngx_http_mongo_connection_t *mongo_conns;
    ngx_uint_t i;

    mongo_conns = ngx_http_mongo_connections.elts;

    for ( i = 0; i < ngx_http_mongo_connections.nelts; i++ ) {
        if ( name.len == mongo_conns[i].name.len
             && ngx_strncmp(name.data, mongo_conns[i].name.data, name.len) == 0 ) {
            return &mongo_conns[i];
        }
    }

    return NULL;
}

static ngx_int_t ngx_http_mongo_authenticate(ngx_log_t *log, ngx_http_gridfs_loc_conf_t *gridfs_loc_conf) {
    ngx_http_mongo_connection_t* mongo_conn;
    ngx_http_mongo_auth_t *mongo_auth;
    bson empty;
    char test[128];
    int error;

    mongo_conn = ngx_http_get_mongo_connection( gridfs_loc_conf->mongo );
    if (mongo_conn == NULL) {
        ngx_log_error(NGX_LOG_ERR, log, 0,
                  "Mongo Connection not found: \"%V\"", &gridfs_loc_conf->mongo);
    }

    // Authenticate
    if (gridfs_loc_conf->user.data != NULL && gridfs_loc_conf->pass.data != NULL) {
        if (!mongo_cmd_authenticate( &mongo_conn->conn, 
                                     (const char*)gridfs_loc_conf->db.data, 
                                     (const char*)gridfs_loc_conf->user.data, 
                                     (const char*)gridfs_loc_conf->pass.data )) {
            ngx_log_error(NGX_LOG_ERR, log, 0,
                          "Invalid mongo user/pass: %s/%s", 
                          gridfs_loc_conf->user.data, 
                          gridfs_loc_conf->pass.data);
            return NGX_ERROR;
        }

        mongo_auth = ngx_array_push(mongo_conn->auths);
        mongo_auth->db = gridfs_loc_conf->db;
        mongo_auth->user = gridfs_loc_conf->user;
        mongo_auth->pass = gridfs_loc_conf->pass;
    }

    // Run a test command to test authentication.
    ngx_cpystrn((u_char*)test, (u_char*)gridfs_loc_conf->db.data, gridfs_loc_conf->db.len+1);
    ngx_cpystrn((u_char*)(test+gridfs_loc_conf->db.len),(u_char*)".test", sizeof(".test"));
    bson_empty(&empty);
    mongo_find(&mongo_conn->conn, test, &empty, NULL, 0, 0, 0);
    error =  mongo_cmd_get_last_error(&mongo_conn->conn, (char*)gridfs_loc_conf->db.data, NULL);
    if (error) {
        ngx_log_error(NGX_LOG_ERR, log, 0, "Authentication Required");
        return NGX_ERROR;
    }

    return NGX_OK;
}

static ngx_int_t ngx_http_mongo_init_gridfs(ngx_cycle_t* cycle, ngx_http_gridfs_loc_conf_t* gridfs_loc_conf) {
	ngx_http_mongo_connection_t* mongo_conn;

	mongo_conn = ngx_http_get_mongo_connection( gridfs_loc_conf->mongo );
	if (mongo_conn == NULL) {
		ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
			"Mongo Connection not found: \"%V\"", &gridfs_loc_conf->mongo);
	}
	if (!gridfs_init(&mongo_conn->conn,
		(const char*)gridfs_loc_conf->db.data,
		(const char*)gridfs_loc_conf->root_collection.data,
		&mongo_conn->gfs)) {
		ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
			"Mongo gridfs initialize failed: \"%V\"", &gridfs_loc_conf->mongo);
	}
	return NGX_OK;
}
static ngx_int_t ngx_http_mongo_add_connection(ngx_cycle_t* cycle, ngx_http_gridfs_loc_conf_t* gridfs_loc_conf) {
    ngx_http_mongo_connection_t* mongo_conn;
    mongo_conn_return status;
    ngx_http_mongod_server_t *mongods;
    volatile ngx_uint_t i;
    u_char host[255];

    mongods = gridfs_loc_conf->mongods->elts;

    mongo_conn = ngx_http_get_mongo_connection( gridfs_loc_conf->mongo );
    if (mongo_conn != NULL) {
        return NGX_OK;
    }

    mongo_conn = ngx_array_push(&ngx_http_mongo_connections);
    if (mongo_conn == NULL) {
        return NGX_ERROR;
    }

    mongo_conn->name = gridfs_loc_conf->mongo;
    mongo_conn->auths = ngx_array_create(cycle->pool, 4, sizeof(ngx_http_mongo_auth_t));

    if ( gridfs_loc_conf->mongods->nelts == 1 ) {
        ngx_cpystrn( host, mongods[0].host.data, mongods[0].host.len + 1 );
        status = mongo_connect( &mongo_conn->conn, (const char*)host, mongods[0].port );
    } else if ( gridfs_loc_conf->mongods->nelts >= 2 && gridfs_loc_conf->mongods->nelts < 9 ) {

        /* Initiate replica set connection. */
        mongo_replset_init_conn( &mongo_conn->conn, (const char *)gridfs_loc_conf->replset.data );

        /* Add replica set seeds. */
        for( i=0; i<gridfs_loc_conf->mongods->nelts; ++i ) {
            ngx_cpystrn( host, mongods[i].host.data, mongods[i].host.len + 1 );
            mongo_replset_add_seed( &mongo_conn->conn, (const char *)host, mongods[i].port );
        }
        status = mongo_replset_connect( &mongo_conn->conn );
    } else {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                          "Mongo Nginx Exception: Too many strings provided in 'mongo' directive.");
        return NGX_ERROR;
    }

    switch (status) {
        case mongo_conn_success:
            break;
        case mongo_conn_bad_arg:
            ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                          "Mongo Exception: Bad Arguments");
            return NGX_ERROR;
        case mongo_conn_no_socket:
            ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                          "Mongo Exception: No Socket");
            return NGX_ERROR;
        case mongo_conn_fail:
            ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                          "Mongo Exception: Connection Failure.");
            return NGX_ERROR;
        case mongo_conn_not_master:
            ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                          "Mongo Exception: Not Master");
            return NGX_ERROR;
        case mongo_conn_bad_set_name:
            ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                          "Mongo Exception: Replica set name %s does not match.", gridfs_loc_conf->replset.data);
            return NGX_ERROR;
        case mongo_conn_cannot_find_primary:
            ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                          "Mongo Exception: Cannot connect to primary node.");
            return NGX_ERROR;
        default:
            ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                          "Mongo Exception: Unknown Error");
            return NGX_ERROR;
    }

    return ngx_http_mongo_init_gridfs(cycle, gridfs_loc_conf);
}

static ngx_int_t ngx_http_gridfs_init_worker(ngx_cycle_t* cycle) {
    ngx_http_gridfs_main_conf_t* gridfs_main_conf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_upload_module);
    ngx_http_gridfs_loc_conf_t** gridfs_loc_confs;
    ngx_uint_t i;

    signal(SIGPIPE, SIG_IGN);

    gridfs_loc_confs = gridfs_main_conf->loc_confs.elts;

    ngx_array_init(&ngx_http_mongo_connections, cycle->pool, 4, sizeof(ngx_http_mongo_connection_t));

    for (i = 0; i < gridfs_main_conf->loc_confs.nelts; i++) {
        if (ngx_http_mongo_add_connection(cycle, gridfs_loc_confs[i]) == NGX_ERROR) {
            return NGX_ERROR;
        }
        if (ngx_http_mongo_authenticate(cycle->log, gridfs_loc_confs[i]) == NGX_ERROR) {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

static ngx_int_t ngx_http_mongo_reconnect(ngx_log_t *log, ngx_http_mongo_connection_t *mongo_conn) {
    volatile mongo_conn_return status = mongo_conn_fail;

    MONGO_TRY_GENERIC(&mongo_conn->conn) {
        if(&mongo_conn->conn.connected) { mongo_disconnect(&mongo_conn->conn); }
        ngx_msleep(MONGO_RECONNECT_WAITTIME);
        status = mongo_reconnect(&mongo_conn->conn);
    } MONGO_CATCH_GENERIC(&mongo_conn->conn) { 
        status = mongo_conn_fail;
    }

    switch (status) {
        case mongo_conn_success:
            break;
        case mongo_conn_bad_arg:
            ngx_log_error(NGX_LOG_ERR, log, 0,
                          "Mongo Exception: Bad Arguments");
            return NGX_ERROR;
        case mongo_conn_no_socket:
            ngx_log_error(NGX_LOG_ERR, log, 0,
                          "Mongo Exception: No Socket");
            return NGX_ERROR;
        case mongo_conn_fail:
            ngx_log_error(NGX_LOG_ERR, log, 0,
                          "Mongo Exception: Connection Failure %s:%i;",
                          mongo_conn->conn.primary->host,
                          mongo_conn->conn.primary->port);
            return NGX_ERROR;
        case mongo_conn_not_master:
            ngx_log_error(NGX_LOG_ERR, log, 0,
                          "Mongo Exception: Not Master");
            return NGX_ERROR;
        default:
            ngx_log_error(NGX_LOG_ERR, log, 0,
                          "Mongo Exception: Unknown Error");
            return NGX_ERROR;
    }
    
    return NGX_OK;
}

static ngx_int_t ngx_http_mongo_reauth(ngx_log_t *log, ngx_http_mongo_connection_t *mongo_conn) {
    ngx_http_mongo_auth_t *auths;
    volatile ngx_uint_t i, success = 0;
    auths = mongo_conn->auths->elts;

    for (i = 0; i < mongo_conn->auths->nelts; i++) {
        MONGO_TRY_GENERIC(&mongo_conn->conn)  {
            success = mongo_cmd_authenticate( &mongo_conn->conn, 
                                              (const char*)auths[i].db.data, 
                                              (const char*)auths[i].user.data, 
                                              (const char*)auths[i].pass.data );
        } MONGO_CATCH_GENERIC(&mongo_conn->conn) { 
            ngx_log_error(NGX_LOG_ERR, log, 0,
                          "Mongo connection error, during reauth");
            return NGX_ERROR;
        }
        if (!success) {
            ngx_log_error(NGX_LOG_ERR, log, 0,
                          "Invalid mongo user/pass: %s/%s, during reauth", 
                          auths[i].user.data, 
                          auths[i].pass.data);   
            return NGX_ERROR;
        }
    }
    
    return NGX_OK;
}

static char h_digit(char hex) {
    return (hex >= '0' && hex <= '9') ? hex - '0': ngx_tolower(hex)-'a'+10;
}

static int htoi(char* h) {
    char ok[] = "0123456789AaBbCcDdEeFf";

    if (ngx_strchr(ok, h[0]) == NULL || ngx_strchr(ok,h[1]) == NULL) { return -1; }
    return h_digit(h[0])*16 + h_digit(h[1]);
}

static int url_decode(char * filename) {
    char * read = filename;
    char * write = filename;
    char hex[3];
    int c;

    hex[2] = '\0';
    while (*read != '\0'){
        if (*read == '%') {
            hex[0] = *(++read);
            if (hex[0] == '\0') return 0;
            hex[1] = *(++read);
            if (hex[1] == '\0') return 0;
            c = htoi(hex);
            if (c == -1) return 0;
            *write = (char)c;
        }
        else *write = *read;
        read++;
        write++;
    }
    *write = '\0';
    return 1;
}

static ngx_int_t ngx_http_gridfs_handler(ngx_http_request_t* request) {
    ngx_http_gridfs_loc_conf_t* gridfs_conf;
    ngx_http_core_loc_conf_t* core_conf;
    ngx_buf_t* buffer;
    ngx_chain_t out;
    ngx_str_t location_name;
    ngx_str_t full_uri;
    char* value;
    ngx_http_mongo_connection_t *mongo_conn;
    gridfile gfile;
    gridfs_offset length;
    ngx_uint_t chunksize;
    ngx_uint_t numchunks;
    char* contenttype;
    volatile ngx_uint_t i;
    volatile ngx_int_t found = 0;
    ngx_int_t rc = NGX_OK;
    bson query;
    bson_buffer buf;
    bson_oid_t oid;
    mongo_cursor ** cursors;
    gridfs_offset chunk_len;
    const char * chunk_data;
    bson_iterator it;
    bson chunk;
    ngx_pool_cleanup_t* gridfs_cln;
    ngx_http_gridfs_cleanup_t* gridfs_clndata;
    volatile ngx_uint_t e = FALSE; 
    volatile ngx_uint_t ecounter = 0;

    gridfs_conf = ngx_http_get_module_loc_conf(request, ngx_http_upload_module);
    core_conf = ngx_http_get_module_loc_conf(request, ngx_http_core_module);

    // ---------- ENSURE MONGO CONNECTION ---------- //

    mongo_conn = ngx_http_get_mongo_connection( gridfs_conf->mongo );
    if (mongo_conn == NULL) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                      "Mongo Connection not found: \"%V\"", &gridfs_conf->mongo);
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    
    if ( !(&mongo_conn->conn.connected)
         && (ngx_http_mongo_reconnect(request->connection->log, mongo_conn) == NGX_ERROR
             || ngx_http_mongo_reauth(request->connection->log, mongo_conn) == NGX_ERROR)) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                      "Could not connect to mongo: \"%V\"", &gridfs_conf->mongo);
        if(&mongo_conn->conn.connected) { mongo_disconnect(&mongo_conn->conn); }
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }

	if (request->method & NGX_HTTP_POST)
		return ngx_http_upload_handler(request);

    // ---------- RETRIEVE KEY ---------- //

    location_name = core_conf->name;
    full_uri = request->uri;

    if (full_uri.len < location_name.len) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                      "Invalid location name or uri.");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    value = ngx_pcalloc(request->pool, full_uri.len - location_name.len + 1);
    if (value == NULL) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                      "Failed to allocate memory for value buffer.");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    memcpy(value, full_uri.data + location_name.len, full_uri.len - location_name.len);
    value[full_uri.len - location_name.len] = '\0';

    if (!url_decode(value)) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                      "Malformed request.");
        return NGX_HTTP_BAD_REQUEST;
    }

    // ---------- RETRIEVE GRIDFILE ---------- //

    bson_buffer_init(&buf);
    switch (gridfs_conf->type) {
    case  bson_oid:
        bson_oid_from_string(&oid, value);
        bson_append_oid(&buf, (char*)gridfs_conf->field.data, &oid);
        break;
    case bson_int:
      bson_append_int(&buf, (char*)gridfs_conf->field.data, ngx_atoi((u_char*)value, strlen(value)));
        break;
    case bson_string:
        bson_append_string(&buf, (char*)gridfs_conf->field.data, value);
        break;
    }
    bson_from_buffer(&query, &buf);

    do {
        MONGO_TRY_GENERIC(&mongo_conn->conn){
            e = FALSE;
            found = gridfs_find_query(&mongo_conn->gfs, &query, &gfile);
        } MONGO_CATCH_GENERIC(&mongo_conn->conn) {
            e = TRUE; ecounter++;
            if (ecounter > MONGO_MAX_RETRIES_PER_REQUEST 
                || ngx_http_mongo_reconnect(request->connection->log, mongo_conn) == NGX_ERROR
                || ngx_http_mongo_reauth(request->connection->log, mongo_conn) == NGX_ERROR) {
                ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                              "Mongo connection dropped, could not reconnect");
                if(&mongo_conn->conn.connected) { mongo_disconnect(&mongo_conn->conn); }
                bson_destroy(&query);
                return NGX_HTTP_SERVICE_UNAVAILABLE;
            }
        }
    } while (e);
    
    bson_destroy(&query);

    if(!found){
        return NGX_HTTP_NOT_FOUND;
    }

    /* Get information about the file */
    length = gridfile_get_contentlength(&gfile);
    chunksize = gridfile_get_chunksize(&gfile);
    numchunks = gridfile_get_numchunks(&gfile);
    contenttype = (char*)gridfile_get_contenttype(&gfile);

    // ---------- SEND THE HEADERS ---------- //

    request->headers_out.status = NGX_HTTP_OK;
    request->headers_out.content_length_n = length;
    if (contenttype != NULL) {
        request->headers_out.content_type.len = strlen(contenttype);
        request->headers_out.content_type.data = (u_char*)contenttype;
    }
    else ngx_http_set_content_type(request);

    /* Determine if content is gzipped, set headers accordingly */
    if ( gridfile_get_boolean(&gfile,"gzipped") ) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0, gridfile_get_field(&gfile,"gzipped") );
        request->headers_out.content_encoding = ngx_list_push(&request->headers_out.headers);
        if (request->headers_out.content_encoding == NULL) {
            return NGX_ERROR;
        }
        request->headers_out.content_encoding->hash = 1;
        request->headers_out.content_encoding->key.len = sizeof("Content-Encoding") - 1;
        request->headers_out.content_encoding->key.data = (u_char *) "Content-Encoding";
        request->headers_out.content_encoding->value.len = sizeof("gzip") - 1;
        request->headers_out.content_encoding->value.data = (u_char *) "gzip";
    }

    ngx_http_send_header(request);

    // ---------- SEND THE BODY ---------- //

    /* Empty file */
    if (numchunks == 0) {
        /* Allocate space for the response buffer */
        buffer = ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
        if (buffer == NULL) {
            ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                          "Failed to allocate response buffer");
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        buffer->pos = NULL;
        buffer->last = NULL;
        buffer->memory = 1;
        buffer->last_buf = 1;
        out.buf = buffer;
        out.next = NULL;
        return ngx_http_output_filter(request, &out);
    }
    
    cursors = (mongo_cursor **)ngx_pcalloc(request->pool, sizeof(mongo_cursor *) * numchunks);
    if (cursors == NULL) {
      return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    
    ngx_memzero( cursors, sizeof(mongo_cursor *) * numchunks);

    /* Hook in the cleanup function */
    gridfs_cln = ngx_pool_cleanup_add(request->pool, sizeof(ngx_http_gridfs_cleanup_t));
    if (gridfs_cln == NULL) {
      return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    gridfs_cln->handler = ngx_http_gridfs_cleanup;
    gridfs_clndata = gridfs_cln->data;
    gridfs_clndata->cursors = cursors;
    gridfs_clndata->numchunks = numchunks;

    /* Read and serve chunk by chunk */
    for (i = 0; i < numchunks; i++) {

        /* Allocate space for the response buffer */
        buffer = ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
        if (buffer == NULL) {
            ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                          "Failed to allocate response buffer");
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        /* Fetch the chunk from mongo */
        do {
            MONGO_TRY_GENERIC(&mongo_conn->conn){
                e = FALSE;
                cursors[i] = gridfile_get_chunks(&gfile, i, 1);
                mongo_cursor_next(cursors[i]);
            } MONGO_CATCH_GENERIC(&mongo_conn->conn) {
                e = TRUE; ecounter++;
                if (ecounter > MONGO_MAX_RETRIES_PER_REQUEST 
                    || ngx_http_mongo_reconnect(request->connection->log, mongo_conn) == NGX_ERROR
                    || ngx_http_mongo_reauth(request->connection->log, mongo_conn) == NGX_ERROR) {
                    ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                                  "Mongo connection dropped, could not reconnect");
                    if(&mongo_conn->conn.connected) { mongo_disconnect(&mongo_conn->conn); }
                    return NGX_HTTP_SERVICE_UNAVAILABLE;
                }
            }
        } while (e);

        chunk = cursors[i]->current;
        bson_find(&it, &chunk, "data");
        chunk_len = bson_iterator_bin_len( &it );
        chunk_data = bson_iterator_bin_data( &it );

        /* Set up the buffer chain */
        buffer->pos = (u_char*)chunk_data;
        buffer->last = (u_char*)chunk_data + chunk_len;
        buffer->memory = 1;
        buffer->last_buf = (i == numchunks-1);
        out.buf = buffer;
        out.next = NULL;

        /* Serve the Chunk */
        rc = ngx_http_output_filter(request, &out);

        /* TODO: More Codes to Catch? */
        if (rc == NGX_ERROR) {
            return NGX_ERROR;
        }
    }

    return rc;
}

static void ngx_http_gridfs_cleanup(void* data) {
    ngx_http_gridfs_cleanup_t* gridfs_clndata;
    volatile ngx_uint_t i;

    gridfs_clndata = data;

    for (i = 0; i < gridfs_clndata->numchunks; i++) {
        mongo_cursor_destroy(gridfs_clndata->cursors[i]);
    }
}


static ngx_int_t
ngx_http_upload_response_error(ngx_http_request_t *r, ngx_int_t error) {
	ngx_int_t rc;
	ngx_buf_t *b;
	ngx_chain_t out;

	r->headers_out.content_type.len = sizeof("text/json") - 1;
	r->headers_out.content_type.data = (u_char *)"text/json";
	r->headers_out.status = NGX_HTTP_OK;
	r->headers_out.content_length_n = sizeof("{\"errcode\":00000,\"errmsg\":\"\"}") - 1;
	rc = ngx_http_send_header(r);
	if (rc == NGX_ERROR || rc > NGX_OK || r->header_only)
		return rc;

	if (!(b = ngx_create_temp_buf(r->pool, r->headers_out.content_length_n))) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "Failed to allocate response buffer.");
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	b->last = ngx_sprintf(b->pos,
		"{\"errcode\":%d,\"errmsg\":\"\"}",
		error);
	b->memory = 1;
	b->last_buf = 1;
	out.buf = b;
	out.next = NULL;
	return ngx_http_output_filter(r, &out);
}

static ngx_int_t
ngx_http_upload_handler(ngx_http_request_t *r) {
	ngx_http_gridfs_loc_conf_t *ulcf;
	ngx_http_gridfs_ctx_t      *ctx;
	ngx_int_t                   rc;

	ulcf = ngx_http_get_module_loc_conf(r, ngx_http_upload_module);

	/* Set upload context if necessary */
	if (!(ctx = ngx_http_get_module_ctx(r, ngx_http_upload_module))) {
		if (!(ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_gridfs_ctx_t))))
			return ngx_http_upload_response_error(r,
				NGX_HTTP_INTERNAL_SERVER_ERROR);
		ngx_http_set_ctx(r, ctx, ngx_http_upload_module);
	}

	/* Check whether Content-Type header is missing */
	if (!r->headers_in.content_type) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, ngx_errno,
			"missing Content-Type header");
		return ngx_http_upload_response_error(r, NGX_HTTP_BAD_REQUEST);
	}

	ctx->request = r;
	ctx->log = r->connection->log;
	ctx->pool = r->pool;
	ctx->chain = ctx->last = ctx->checkpoint = NULL;

	upload_initialize_ctx(ctx);

	/* Check multipart/form-data and get boundary */
	if (NGX_OK != upload_parse_content_type(ctx,
		&r->headers_in.content_type->value))
		return ngx_http_upload_response_error(r, NGX_HTTP_BAD_REQUEST);

	/* Allocate buf for part header */
	if (NGX_OK != upload_prepare_part(ctx, ulcf)) {
		return ngx_http_upload_response_error(r,
			NGX_HTTP_INTERNAL_SERVER_ERROR);
	}

	if (NGX_HTTP_SPECIAL_RESPONSE <= (
		rc = ngx_http_upload_read_client_request_body(r))) {
		return ngx_http_upload_response_error(r, rc);
	}

	return NGX_DONE;
}

static ngx_int_t
ngx_http_upload_do_response_handler(ngx_http_request_t *r) {
	ngx_int_t              rc;
	ngx_buf_t             *b;
	ngx_chain_t           *cl, out;
	ngx_http_gridfs_ctx_t *ctx;

	r->headers_out.content_type.len = sizeof("text/json") - 1;
	r->headers_out.content_type.data = (u_char *)"text/json";
	r->headers_out.status = NGX_HTTP_OK;
	r->headers_out.content_length_n = sizeof("[");

	if (!(b = ngx_create_temp_buf(r->pool, r->headers_out.content_length_n))) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "Failed to allocate response buffer.");
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	*b->last++ = '[';
	b->memory = 1;
	b->last_buf = 0;

	out.buf = b;
	out.next = NULL;
	ctx = ngx_http_get_module_ctx(r, ngx_http_upload_module);
	if ((ctx->media)) {
		for (cl = ctx->media; cl; cl = cl->next) {
			b = cl->buf;
			*b->last++ = ',';
			r->headers_out.content_length_n += b->last - b->pos;
		}
		--b->last;
		--r->headers_out.content_length_n;
		out.next = ctx->media;
	}
	*b->last++ = ']';
	b->last_buf = 1;
	rc = ngx_http_send_header(r);
	if (rc == NGX_ERROR || rc > NGX_OK || r->header_only)
		return rc;
	return ngx_http_output_filter(r, &out);
}

static void
ngx_http_upload_response_handler(ngx_http_request_t *r) {
	ngx_http_finalize_request(r, ngx_http_upload_do_response_handler(r));
}

static ngx_int_t
ngx_http_upload_test_expect(ngx_http_request_t *r) {
	ngx_str_t  *expect;

	if (r->expect_tested
		|| !r->headers_in.expect
		|| r->http_version < NGX_HTTP_VERSION_11)
		return NGX_OK;
	r->expect_tested = 1;

	expect = &r->headers_in.expect->value;
	if (expect->len != sizeof("100-continue") - 1
		|| ngx_strncasecmp(expect->data, (u_char *) "100-continue",
			sizeof("100-continue") - 1))
		return NGX_OK;

	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
		"send 100 Continue");

	if (sizeof("HTTP/1.1 100 Continue" CRLF CRLF) - 1 == r->connection->send(
		r->connection, (u_char *)"HTTP/1.1 100 Continue" CRLF CRLF,
		sizeof("HTTP/1.1 100 Continue" CRLF CRLF) - 1))
		return NGX_OK;

	/* we assume that such small packet should be send successfully */
	return NGX_ERROR;
}

static ngx_int_t
ngx_http_upload_read_client_request_body_agent(ngx_http_request_t *r) {
	ssize_t                    size, preread;
	ngx_buf_t                 *b;
	ngx_http_request_body_t   *rb;
	ngx_http_core_loc_conf_t  *clcf;
	ngx_http_gridfs_ctx_t     *ctx;

	ctx = ngx_http_get_module_ctx(r, ngx_http_upload_module);

	if (NGX_OK != ngx_http_upload_test_expect(r))
		return NGX_HTTP_INTERNAL_SERVER_ERROR;

	if (!(rb = ngx_pcalloc(r->pool, sizeof(ngx_http_request_body_t))))
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	r->request_body = rb;

	if (r->headers_in.content_length_n <= 0 || r->headers_in.chunked)
		return NGX_HTTP_BAD_REQUEST;

	/*
	 * set by ngx_pcalloc():
	 *
	 *     rb->bufs = NULL;
	 *     rb->buf = NULL;
	 *     rb->rest = 0;
	 */
	b = NULL;
	rb->rest = r->headers_in.content_length_n;
	if ((preread = r->header_in->last - r->header_in->pos)) {
		/* there is the pre-read part of the request body */
		ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"http client request body preread %uz", preread);

		if (!(b = ngx_calloc_buf(r->pool)))
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		b->temporary = 1;
		b->start = r->header_in->pos;
		b->pos = r->header_in->pos;
		b->last = r->header_in->last;
		b->end = r->header_in->end;
		rb->buf = b;

		if (r->headers_in.content_length_n <= preread) {
			/* the whole request body was pre-read */
			r->header_in->pos += r->headers_in.content_length_n;
			r->request_length += r->headers_in.content_length_n;
			b->last_buf = 1;
			if (NGX_OK != ngx_http_upload_process_client_request_body(r)) {
				upload_shutdown_ctx(ctx);
				return NGX_HTTP_INTERNAL_SERVER_ERROR;
			}
			ngx_http_upload_response_handler(r);
			return NGX_OK;
		}

		/*
		 * to not consider the body as pipelined request in
		 * ngx_http_set_keepalive()
		 */
		r->header_in->pos = r->header_in->last;
		r->request_length += preread;
		rb->rest -= preread;
		if (rb->rest <= (off_t)(b->end - b->last)) {
			/* the whole request body may be placed in r->header_in */
			rb->buf = b;
			b->last_buf = 1;
			r->read_event_handler = ngx_http_read_upload_client_request_body_handler;
			return ngx_http_upload_do_read_client_request_body(r);
		}

		b->last_buf = 0;
		if (!r->request_body_in_single_buf
			&& NGX_OK != ngx_http_upload_process_client_request_body(r)) {
			upload_shutdown_ctx(ctx);
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
	}

	clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);

	size = clcf->client_body_buffer_size;
	size += size >> 2;
	if (rb->rest < (off_t)size) {
		size = rb->rest;
		if (r->request_body_in_single_buf) size += preread;
	} else size = clcf->client_body_buffer_size;

	if (!(rb->buf = ngx_create_temp_buf(r->pool, size))) {
		upload_shutdown_ctx(ctx);
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	rb->buf->last_buf = 0;

	if (b && r->request_body_in_single_buf) {
		size = b->last - b->pos;
		ngx_memcpy(rb->buf->pos, b->pos, size);
		rb->buf->last += size;
	}

	r->read_event_handler = ngx_http_read_upload_client_request_body_handler;
	return ngx_http_upload_do_read_client_request_body(r);
}

static ngx_int_t
ngx_http_upload_read_client_request_body(ngx_http_request_t *r) {
	ngx_int_t rc;

	++r->main->count;
	if (r->request_body || r->discard_body) return NGX_OK;
	if (NGX_HTTP_SPECIAL_RESPONSE <= (
		rc = ngx_http_upload_read_client_request_body_agent(r)))
		--r->main->count;
	return rc;
}

static void
ngx_http_read_upload_client_request_body_handler(ngx_http_request_t *r) {
	ngx_int_t                  rc;
	ngx_http_gridfs_ctx_t     *ctx;

	ctx = ngx_http_get_module_ctx(r, ngx_http_upload_module);

	if (r->connection->read->timedout) {
		r->connection->timedout = 1;
		upload_shutdown_ctx(ctx);
		ngx_http_finalize_request(r, NGX_HTTP_REQUEST_TIME_OUT);
		return;
	}

	if (NGX_HTTP_SPECIAL_RESPONSE <= (rc = 
		ngx_http_upload_do_read_client_request_body(r))) {
		upload_shutdown_ctx(ctx);
		ngx_http_finalize_request(r, rc);
	}
}

static ngx_int_t
ngx_http_upload_do_read_client_request_body(ngx_http_request_t *r) {
	size_t                     size;
	ssize_t                    n;
	ngx_connection_t          *c;
	ngx_http_request_body_t   *rb;
	ngx_http_core_loc_conf_t  *clcf;
	ngx_http_gridfs_ctx_t     *ctx;
	ngx_int_t                  rc;

	c = r->connection;
	rb = r->request_body;
	ctx = ngx_http_get_module_ctx(r, ngx_http_upload_module);

	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0,
		"http read client request body");

	for (;;) {
		for (;;) {
			if (rb->buf->last == rb->buf->end) {
				rc = ngx_http_upload_process_client_request_body(r);
				switch (rc) {
				case NGX_OK: break;
				case NGX_UPLOAD_MALFORMED: return NGX_HTTP_BAD_REQUEST;
				case NGX_UPLOAD_IOERROR: return NGX_HTTP_SERVICE_UNAVAILABLE;
				case NGX_UPLOAD_NOMEM: case NGX_UPLOAD_SCRIPTERROR: default:
					return NGX_HTTP_INTERNAL_SERVER_ERROR;
				}
				rb->buf->last = rb->buf->start;
			}
			size = rb->buf->end - rb->buf->last;
			if (rb->rest < (off_t)size) size = rb->rest;

			n = c->recv(c, rb->buf->last, size);

			ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0,
				"http client request body recv %z", n);

			if (n == NGX_AGAIN) break;

			if (n == 0)
				ngx_log_error(NGX_LOG_INFO, c->log, 0,
					"client closed prematurely connection");

			if (n == 0 || n == NGX_ERROR) {
				c->error = 1;
				return NGX_HTTP_BAD_REQUEST;
			}

			rb->buf->last += n;
			r->request_length += n;
			if (!(rb->rest -= n)) break;
			if (rb->buf->last < rb->buf->end) break;
		}

		ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0,
			"http client request body rest %uz", rb->rest);

		if (!rb->rest) {
			rb->buf->last_buf = 1;
			break;
		}

		if (!c->read->ready) {
			clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
			ngx_add_timer(c->read, clcf->client_body_timeout);

			if (NGX_OK != ngx_handle_read_event(c->read, 0))
				return NGX_HTTP_INTERNAL_SERVER_ERROR;
			return NGX_AGAIN;
		}
	}

	if (c->read->timer_set) ngx_del_timer(c->read);
	r->read_event_handler = ngx_http_block_reading;
	if (NGX_OK != ngx_http_upload_process_client_request_body(r))
		return NGX_HTTP_INTERNAL_SERVER_ERROR;

	ngx_http_upload_response_handler(r);
	return NGX_OK;
}

static ngx_int_t
ngx_http_upload_process_client_request_body(ngx_http_request_t *r) {
	ngx_http_gridfs_ctx_t     *ctx;
	ngx_int_t                  rc;
	ngx_http_request_body_t   *rb;
	ngx_buf_t                 *b;

	rb = r->request_body;
	ctx = ngx_http_get_module_ctx(r, ngx_http_upload_module);

	/* Feed all the buffers into multipart/form-data processor */
	b = rb->buf;
	if (NGX_OK != (rc = upload_process_buf(ctx, b->pos, b->last)))
		return rc;
	b->last = b->start;

	/* Signal end of body */
	if (b->last_buf && upload_state_finish != ctx->state)
		return NGX_UPLOAD_MALFORMED;

	return NGX_OK;
}

static ngx_int_t
ngx_http_upload_append_media(ngx_http_gridfs_ctx_t *ctx,
	const char *media_id, ngx_int_t upload_date) {
	ngx_int_t    len;
	ngx_chain_t *cl;
	ngx_buf_t   *b;

	len = sizeof("{\"media_id\":\"000000000000000000000000\",\"created_at\":0000000000}");
	if (!(b = ngx_create_temp_buf(ctx->pool, len)))
		return NGX_UPLOAD_NOMEM;
	if (!(cl = ngx_alloc_chain_link(ctx->pool)))
		return NGX_UPLOAD_NOMEM;
	b->last = ngx_sprintf(b->last,
		"{\"media_id\":\"%s\",\"created_at\":%010uD}",
		media_id, upload_date);
	b->last_in_chain = 0;

	cl->buf = b;
	cl->next = NULL;
	if (ctx->media) {
		ctx->media_last->next = cl;
		ctx->media_last = cl;
	} else {
		ctx->media = cl;
		ctx->media_last = cl;
	}

	return NGX_OK;
}

static ngx_int_t
ngx_http_upload_start_handler(ngx_http_gridfs_ctx_t *ctx) {
	ngx_http_request_t         *r = ctx->request;
	ngx_http_gridfs_loc_conf_t *ulcf;

	ngx_file_t *file;
	ngx_path_t *path;
	uint32_t    n;
	ngx_uint_t  i;
	ngx_int_t   rc;
	ngx_err_t   err;
	ngx_http_upload_field_template_t    *t;
	ngx_str_t   field_name, field_value;

	ulcf = ngx_http_get_module_loc_conf(r, ngx_http_upload_module);
	path = ulcf->store_path;
	file = &ctx->output_file;

	if (ctx->is_file) {
		file->name.len = path->name.len + 1 + path->len + 10;
		if (!(file->name.data = ngx_palloc(ctx->pool, file->name.len + 1)))
			return NGX_UPLOAD_NOMEM;
		ngx_memcpy(file->name.data, path->name.data, path->name.len);

		file->log = ctx->log;
		for (;;) {
			n = (uint32_t)ngx_next_temp_number(0);
			ngx_sprintf(file->name.data + path->name.len + 1 + path->len,
				"%010uD%Z", n);
			ngx_create_hashed_filename(path, file->name.data, file->name.len);
			ngx_log_debug1(NGX_LOG_DEBUG_CORE, file->log, 0,
				"hashed path: %s", file->name.data);

			if (NGX_INVALID_FILE != (file->fd = ngx_open_tempfile(
				file->name.data, 1, ulcf->store_access))) {
				file->offset = 0;
				break;
			}

			err = ngx_errno;
			if (err == NGX_EEXIST) {
				n = (uint32_t) ngx_next_temp_number(1);
				continue;
			}

			ngx_log_error(NGX_LOG_ERR, ctx->log, ngx_errno,
				"failed to create output file for \"%s\"",
				ctx->file_name.data);
			return NGX_UPLOAD_IOERROR;
		}

		if (ulcf->field_templates) {
			t = ulcf->field_templates->elts;
			for (i = 0; i < ulcf->field_templates->nelts; ++i) {

				if (t[i].field_lengths) {
					if (ngx_http_script_run(r, &field_name,
						t[i].field_lengths->elts, 0,
						t[i].field_values->elts) == NULL)
						return NGX_UPLOAD_SCRIPTERROR;
				} else field_name = t[i].value.key;

				if (t[i].value_lengths) {
					if (ngx_http_script_run(r, &field_value,
						t[i].value_lengths->elts, 0,
						t[i].value_values->elts) == NULL)
						return NGX_UPLOAD_SCRIPTERROR;
				} else field_value = t[i].value.value;

				if (NGX_OK != (rc = ngx_http_upload_append_field(ctx,
					&field_name, &field_value))) return rc;
			}
		}

		ngx_log_error(NGX_LOG_INFO, ctx->log, 0,
			"started uploading file \"%s\" to \"%s\""
			" (field \"%s\", content type \"%s\")",
			ctx->file_name.data,
			ctx->output_file.name.data,
			ctx->field_name.data,
			ctx->content_type.data);
	} else {
		/*
		 * Here we do a small hack: the content of a normal field
		 * is not known until ngx_http_upload_flush_buffer
		 * is called. We pass empty field value to simplify things.
		 */
		if (NGX_OK != (rc = ngx_http_upload_append_field(ctx,
			&ctx->field_name, &ngx_http_upload_empty_field_value)))
			return rc;
	}

	return NGX_OK;
}

static void
ngx_http_upload_finish_handler(ngx_http_gridfs_ctx_t *ctx) {
	if (ctx->is_file) {
		ngx_close_file(ctx->output_file.fd);

		ngx_log_error(NGX_LOG_INFO, ctx->log, 0,
			"finished uploading file \"%s\" to \"%s\"",
			ctx->file_name.data, ctx->output_file.name.data);
	}

	/* Checkpoint current output chain state */
	ctx->checkpoint = ctx->last;
}

static void
ngx_http_upload_abort_handler(ngx_http_gridfs_ctx_t *ctx) {
	if (ctx->is_file) {
		ngx_close_file(ctx->output_file.fd);

		if (ngx_delete_file(ctx->output_file.name.data) == NGX_FILE_ERROR) {
			ngx_log_error(NGX_LOG_ERR, ctx->log, ngx_errno,
				"aborted uploading file \"%s\" to \"%s\","
				" failed to remove destination file",
				ctx->file_name.data, ctx->output_file.name.data);
		} else {
			ngx_log_error(NGX_LOG_ALERT, ctx->log, 0,
				"aborted uploading file \"%s\" to \"%s\", dest file removed",
				ctx->file_name.data, ctx->output_file.name.data);
		}
	}

	/* Rollback output chain to the previous consistant state */
	if (ctx->checkpoint) {
		ctx->last = ctx->checkpoint;
		ctx->last->next = NULL;
	} else {
		ctx->chain = ctx->last = NULL;
		ctx->first_part = 1;
	}
}

static ngx_int_t
ngx_http_upload_flush_buffer(ngx_http_gridfs_ctx_t *ctx,
	u_char *buf, size_t len) {
	ngx_buf_t                      *b;
	ngx_chain_t                    *cl;

	if (ctx->is_file) {
		if (NGX_ERROR == ngx_write_file(&ctx->output_file,
			buf, len, ctx->output_file.offset)) {
			ngx_log_error(NGX_LOG_ERR, ctx->log, ngx_errno,
				"write to file \"%s\" failed", ctx->output_file.name.data);
			return NGX_UPLOAD_IOERROR;
		}
	} else {
		if (!(b = ngx_create_temp_buf(ctx->pool, len))) return NGX_ERROR;
		if (!(cl = ngx_alloc_chain_link(ctx->pool))) return NGX_ERROR;
		b->last_in_chain = 0;
		cl->buf = b;
		cl->next = NULL;
		b->last = ngx_cpymem(b->last, buf, len);
		if (ctx->chain) {
			ctx->last->next = cl;
			ctx->last = cl;
		} else {
			ctx->chain = cl;
			ctx->last = cl;
		}
	}
	return NGX_OK;
}

static ngx_int_t
ngx_http_upload_append_field(ngx_http_gridfs_ctx_t *ctx,
	ngx_str_t *name, ngx_str_t *value) {
	ngx_int_t    len;
	ngx_chain_t *cl;
	ngx_buf_t   *b;

	len = ctx->first_part ? ctx->boundary.len - 2 : ctx->boundary.len;
	len += sizeof("\r\nContent-Disposition: form-data; name=\"") - 1;
	len += name->len;
	len += sizeof("\"\r\n\r\n") - 1;
	len += value->len;

	if (!(b = ngx_create_temp_buf(ctx->pool, len)))
		return NGX_UPLOAD_NOMEM;
	if (!(cl = ngx_alloc_chain_link(ctx->pool)))
		return NGX_UPLOAD_NOMEM;

	b->last = ngx_cpymem(b->last, ctx->first_part
		? ctx->boundary.data + 2 : ctx->boundary.data,
		ctx->first_part ? ctx->boundary.len - 2 : ctx->boundary.len);
	b->last = ngx_cpymem(b->last,
		"\r\nContent-Disposition: form-data; name=\"",
		sizeof("\r\nContent-Disposition: form-data; name=\"") - 1);
	b->last = ngx_cpymem(b->last, name->data, name->len);
	b->last = ngx_cpymem(b->last, "\"\r\n\r\n", sizeof("\"\r\n\r\n") - 1);
	b->last = ngx_cpymem(b->last, value->data, value->len);
	b->last_in_chain = 0;

	cl->buf = b;
	cl->next = NULL;

	if (ctx->chain) {
		ctx->last->next = cl;
		ctx->last = cl;
	} else {
		ctx->chain = cl;
		ctx->last = cl;
	}
	ctx->first_part = 0;

	return NGX_OK;
}


static ngx_int_t
upload_parse_part_header(ngx_http_gridfs_ctx_t *ctx,
	char *header, char *header_end) {
	if (!strncasecmp(CONTENT_DISPOSITION_STRING, header,
		sizeof(CONTENT_DISPOSITION_STRING)-1)) {
		char *filename_start, *filename_end;
		char *fieldname_start, *fieldname_end;
		char *p = header + sizeof(CONTENT_DISPOSITION_STRING) - 1;

		p += strspn(p, " ");
		if (strncasecmp(FORM_DATA_STRING, p, sizeof(FORM_DATA_STRING)-1) && 
			strncasecmp(ATTACHMENT_STRING, p, sizeof(ATTACHMENT_STRING)-1)) {
			ngx_log_debug0(NGX_LOG_DEBUG_CORE, ctx->log, 0,
				"Content-Disposition is not form-data or attachment");
			return NGX_UPLOAD_MALFORMED;
		}

		if ((filename_start = strstr(p, FILENAME_STRING))) {
			char *q;
			filename_start += sizeof(FILENAME_STRING)-1;
			filename_end = filename_start + strcspn(filename_start, "\"");
			if (*filename_end != '\"') {
				ngx_log_debug0(NGX_LOG_DEBUG_CORE, ctx->log, 0,
					"malformed filename in part header");
				return NGX_UPLOAD_MALFORMED;
			}

			/*
			 * IE sends full path, strip path from filename 
			 * Also strip all UNIX path references
			 */
			for (q = filename_end-1; filename_start < q; --q)
				if (*q == '\\' || *q == '/') {
					filename_start = q+1;
					break;
				}

			ctx->file_name.len = filename_end - filename_start;
			if (!(ctx->file_name.data = ngx_pcalloc(ctx->pool,
				ctx->file_name.len + 1)))
				return NGX_UPLOAD_NOMEM;
			strncpy((char*)ctx->file_name.data, filename_start,
				filename_end - filename_start);
		}

		fieldname_start = p;
		do {
			fieldname_start = strstr(fieldname_start, FIELDNAME_STRING);
		} while (fieldname_start && (fieldname_start
			+sizeof(FIELDNAME_STRING)-1 == filename_start));

		if (fieldname_start) {
			fieldname_start += sizeof(FIELDNAME_STRING)-1;
			if (fieldname_start != filename_start) {
				fieldname_end = fieldname_start
					+ strcspn(fieldname_start, "\"");
				if (*fieldname_end != '\"') {
					ngx_log_debug0(NGX_LOG_DEBUG_CORE, ctx->log, 0,
						"malformed filename in part header");
					return NGX_UPLOAD_MALFORMED;
				}
				ctx->field_name.len = fieldname_end - fieldname_start;
				if (!(ctx->field_name.data = ngx_pcalloc(ctx->pool,
					ctx->field_name.len + 1)))
					return NGX_UPLOAD_NOMEM;
				strncpy((char*)ctx->field_name.data, fieldname_start,
					fieldname_end - fieldname_start);
			}
		}
	} else if(!strncasecmp(CONTENT_TYPE_STRING, header,
		sizeof(CONTENT_TYPE_STRING)-1)) {
		char *content_type_str = header + sizeof(CONTENT_TYPE_STRING)-1;
		content_type_str += strspn(content_type_str, " ");
		ctx->content_type.len = header_end - content_type_str;
		if (!ctx->content_type.len) {
			ngx_log_debug0(NGX_LOG_DEBUG_CORE, ctx->log, 0,
				"empty Content-Type in part header");
			return NGX_UPLOAD_MALFORMED; /* Empty Content-Type field */
		}

		if (!(ctx->content_type.data = ngx_pcalloc(ctx->pool,
			ctx->content_type.len + 1)))
			return NGX_UPLOAD_NOMEM;
		strncpy((char*)ctx->content_type.data, content_type_str,
			ctx->content_type.len);
	}

	return NGX_OK;
}

static void
upload_discard_part_attributes(ngx_http_gridfs_ctx_t *ctx) {
	ctx->file_name.len = 0;
	ctx->file_name.data = NULL;

	ctx->field_name.len = 0;
	ctx->field_name.data = NULL;

	ctx->content_type.len = 0;
	ctx->content_type.data = NULL;

	ctx->discard_data = 0;
}

static ngx_int_t
upload_start_file(ngx_http_gridfs_ctx_t *ctx) {
	if (ctx->start_part_f) return ctx->start_part_f(ctx);
	else return NGX_OK;
}
static void
upload_finish_file(ngx_http_gridfs_ctx_t *ctx) {
	if (ctx->finish_part_f) ctx->finish_part_f(ctx);
	upload_discard_part_attributes(ctx);
}
static void
upload_abort_file(ngx_http_gridfs_ctx_t *ctx) {
	if (ctx->abort_part_f) ctx->abort_part_f(ctx);
	upload_discard_part_attributes(ctx);
}
static void
upload_flush_buffer(ngx_http_gridfs_ctx_t *ctx) {
	if (ctx->flush_buffer_f
		&& ctx->output_buffer < ctx->output_buffer_pos) {
		if (NGX_OK != ctx->flush_buffer_f(ctx,
			(void*)ctx->output_buffer, 
			(size_t)(ctx->output_buffer_pos - ctx->output_buffer)))
			ctx->discard_data = 1;

		ctx->output_buffer_pos = ctx->output_buffer;	
	}
}

static void
upload_initialize_ctx(ngx_http_gridfs_ctx_t *ctx) {
	ctx->boundary.data = ctx->boundary_start = ctx->boundary_pos = 0;
	ctx->state = upload_state_boundary_seek;

	upload_discard_part_attributes(ctx);

	ctx->start_part_f = ngx_http_upload_start_handler;
	ctx->finish_part_f = ngx_http_upload_finish_handler;
	ctx->abort_part_f = ngx_http_upload_abort_handler;
	ctx->flush_buffer_f = ngx_http_upload_flush_buffer;

	ctx->start_part_f = ngx_http_gridfs_start_handler;
	ctx->finish_part_f = ngx_http_gridfs_finish_handler;
	ctx->abort_part_f = ngx_http_gridfs_abort_handler;
	ctx->flush_buffer_f = ngx_http_gridfs_flush_buffer;
}

static void
upload_shutdown_ctx(ngx_http_gridfs_ctx_t *ctx) {
	if (ctx) {
		/* abort file if we still processing it */
		if (upload_state_data == ctx->state) {
			upload_flush_buffer(ctx);
			upload_abort_file(ctx);
		}
		upload_discard_part_attributes(ctx);
	}
}

static ngx_int_t
upload_prepare_part(ngx_http_gridfs_ctx_t *ctx,
	ngx_http_gridfs_loc_conf_t *ulcf) {
	if (!(ctx->header_accumulator = ngx_pcalloc(ctx->pool,
		ulcf->max_header_len + 1)))
		return NGX_ERROR;
	ctx->header_accumulator_pos = ctx->header_accumulator;
	ctx->header_accumulator_end = ctx->header_accumulator
		+ ulcf->max_header_len;

	if (!(ctx->output_buffer = ngx_pcalloc(ctx->pool, ulcf->buffer_size)))
		return NGX_ERROR;
	ctx->output_buffer_pos = ctx->output_buffer;
	ctx->output_buffer_end = ctx->output_buffer + ulcf->buffer_size;

	ctx->first_part = 1;

	return NGX_OK;
}

static ngx_int_t
upload_parse_content_type(ngx_http_gridfs_ctx_t *ctx,
	ngx_str_t *content_type) {
	/* Find colon in content type string, which terminates mime type */
	char *boundary_start, *boundary_end, *mime_type_end;

	if (!(mime_type_end = strchr((char*)content_type->data, ';'))) {
		ngx_log_debug0(NGX_LOG_DEBUG_CORE, ctx->log, 0,
			"no boundary found in Content-Type");
		return NGX_UPLOAD_MALFORMED;
	}
	if (strncasecmp(MULTIPART_FORM_DATA_STRING, (char*)content_type->data,
		(u_char*)mime_type_end - content_type->data)) {
		ngx_log_debug1(NGX_LOG_DEBUG_CORE, ctx->log, 0,
			"Content-Type is not multipart/form-data: %s",
			content_type->data);
		return NGX_UPLOAD_MALFORMED;
	}

	if (!(boundary_start = strstr(mime_type_end, BOUNDARY_STRING))) {
		ngx_log_debug0(NGX_LOG_DEBUG_CORE, ctx->log, 0,
			"no boundary found in Content-Type");
		return NGX_UPLOAD_MALFORMED; /* No boundary found */
	}
	boundary_start += sizeof(BOUNDARY_STRING) - 1;
	boundary_end = boundary_start + strcspn(boundary_start, " ;\n\r");
	if (boundary_end == boundary_start) {
		ngx_log_debug0(NGX_LOG_DEBUG_CORE, ctx->log, 0,
			"boundary is empty");
		return NGX_UPLOAD_MALFORMED;
	}

	/*
	 * Allocate memory for entire boundary
	 * plus \r\n plus terminating character
	 */
	ctx->boundary.len = boundary_end - boundary_start + 4;
	if (!(ctx->boundary.data = ngx_pcalloc(ctx->pool, ctx->boundary.len+1)))
		return NGX_UPLOAD_NOMEM;
	strncpy((char*)ctx->boundary.data + 4, boundary_start,
		boundary_end - boundary_start);
	ctx->boundary.data[boundary_end - boundary_start + 4] = '\0';
	/* Prepend boundary data by \r\n-- */
	ctx->boundary.data[0] = '\r'; 
	ctx->boundary.data[1] = '\n'; 
	ctx->boundary.data[2] = '-'; 
	ctx->boundary.data[3] = '-'; 

	/*
	 * NOTE: first boundary doesn't start with \r\n. Here we
	 * advance 2 positions forward. We will return 2 positions back later
	 */
	ctx->boundary_pos = ctx->boundary_start = ctx->boundary.data + 2;
	return NGX_OK;
}

static void
upload_putc(ngx_http_gridfs_ctx_t *ctx, u_char c) {
	if (!ctx->discard_data) {
		*ctx->output_buffer_pos = c;
		if (++ctx->output_buffer_pos == ctx->output_buffer_end)
			upload_flush_buffer(ctx);	
	}
}

static ngx_int_t
upload_process_buf(ngx_http_gridfs_ctx_t *ctx, u_char *start, u_char *end) {
	u_char *p;
	ngx_int_t rc;

	for (p = start; p < end; ++p) {
		switch (ctx->state) {
		case upload_state_boundary_seek: /* Seek the boundary */
			if (*p == *ctx->boundary_pos) ++ctx->boundary_pos;
			else ctx->boundary_pos = ctx->boundary_start;

			if (ctx->boundary_pos == ctx->boundary.data + ctx->boundary.len) {
				ctx->state = upload_state_after_boundary;
				ctx->boundary_pos = ctx->boundary_start = ctx->boundary.data;
			}
			break;
		case upload_state_after_boundary:
			switch (*p) {
			case '\n':
				ctx->state = upload_state_headers;
				ctx->header_accumulator_pos = ctx->header_accumulator;
			case '\r': break;
			case '-': ctx->state = upload_state_finish; break;
			}
			break;
		case upload_state_headers: /* Collect and store headers */
			switch (*p) {
			case '\n':
				if (ctx->header_accumulator_pos == ctx->header_accumulator) {
					ctx->is_file = !ctx->file_name.data || !ctx->file_name.len
						? 0 : 1;
					if (NGX_OK != (rc = upload_start_file(ctx))) {
						ctx->state = upload_state_finish;
						return rc; /* User requested to cancel processing */
					}
					ctx->state = upload_state_data;
					ctx->output_buffer_pos = ctx->output_buffer;
				} else {
					*ctx->header_accumulator_pos = '\0';

					if (NGX_OK != (rc = upload_parse_part_header(ctx,
						(char*)ctx->header_accumulator,
						(char*)ctx->header_accumulator_pos))) {
						ctx->state = upload_state_finish;
						return rc; /* Malformed header */
					}
					ctx->header_accumulator_pos = ctx->header_accumulator;
				}
			case '\r': break;
			default:
				if (ctx->header_accumulator_pos
					< ctx->header_accumulator_end-1)
					*ctx->header_accumulator_pos++ = *p;
				else {
					ctx->state = upload_state_finish;
					return NGX_UPLOAD_MALFORMED; /* Header is too long */
				}
				break;
			}
			break;
	   /*
		* Search for separating or terminating boundary
		* and output data simultaneously
		*/
		case upload_state_data:
			if (*p == *ctx->boundary_pos) ++ctx->boundary_pos;
			else {
				if (ctx->boundary_pos == ctx->boundary_start) {
					/* IE 5.0 bug workaround */
					if (*p == '\n') {
		   /*
			* Set current matched position beyond LF and prevent outputting
			* CR in case of unsuccessful match by altering boundary_start 
			*/ 
						ctx->boundary_pos = ctx->boundary.data + 2;
						ctx->boundary_start = ctx->boundary.data + 1;
					} else upload_putc(ctx, *p);
				} else {
					/* Output partially matched lump of boundary */
					u_char *q;
					for (q = ctx->boundary_start; q != ctx->boundary_pos; ++q)
						upload_putc(ctx, *q);

					--p; /* Repeat reading last character */

					/* And reset matched position */
					ctx->boundary_pos = ctx->boundary_start = ctx->boundary.data;
				}
			}

			if (ctx->boundary_pos == ctx->boundary.data + ctx->boundary.len) {
				ctx->state = upload_state_after_boundary;
				ctx->boundary_pos = ctx->boundary_start;

				upload_flush_buffer(ctx);
				if (ctx->discard_data) upload_abort_file(ctx);
				else upload_finish_file(ctx);
			}
			break;
		case upload_state_finish: break; /* Skip trailing garbage */
		}
	}
	return NGX_OK;
}


static ngx_int_t
ngx_http_gridfs_start_handler(ngx_http_gridfs_ctx_t *ctx) {
	ngx_http_mongo_connection_t *mongo_conn;
	ngx_http_gridfs_loc_conf_t *gridfs_loc_conf;
	gridfs_loc_conf = ngx_http_get_module_loc_conf(ctx->request, ngx_http_upload_module);
	mongo_conn = ngx_http_get_mongo_connection( gridfs_loc_conf->mongo );
	if (ctx->is_file) {
		gridfile_writer_init(&ctx->grid_file, &mongo_conn->gfs,
			(const char *)ctx->file_name.data,
			(const char *)ctx->content_type.data);
		ngx_log_error(NGX_LOG_INFO, ctx->log, 0,
			"started uploading file \"%s\""
			" (field \"%s\", content type \"%s\")",
			ctx->file_name.data,
			ctx->field_name.data,
			ctx->content_type.data);
	}
	return NGX_OK;
}

static void
ngx_http_gridfs_finish_handler(ngx_http_gridfs_ctx_t *ctx) {
	//ngx_int_t upload_time;
	char id[25];
	if (ctx->is_file) {
		gridfile_writer_done(&ctx->grid_file);

		ngx_log_error(NGX_LOG_INFO, ctx->log, 0,
			"finished uploading file \"%s\"",
			ctx->file_name.data);

		//upload_time = gridfile_get_uploaddate(&ctx->grid_file) / 1000;
		bson_oid_to_string(&ctx->grid_file.id, id);
		ngx_http_upload_append_media(ctx, id, ngx_time());
	}
}

static void
ngx_http_gridfs_abort_handler(ngx_http_gridfs_ctx_t *ctx) {
	if (ctx->is_file) {
		gridfile_destroy(&ctx->grid_file);
		ngx_log_error(NGX_LOG_ALERT, ctx->log, 0,
			"aborted uploading file \"%s\", dest file removed",
			ctx->file_name.data);
	}
}

static ngx_int_t
ngx_http_gridfs_flush_buffer(ngx_http_gridfs_ctx_t *ctx,
	u_char *buf, size_t len) {

	if (ctx->is_file) {
		gridfile_write_buffer(&ctx->grid_file, (const char *)buf, len);
	}
	return NGX_OK;
}
