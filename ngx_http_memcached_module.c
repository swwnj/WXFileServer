
/*
 * Copyright (C) Qu.
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>


typedef struct {
	ngx_http_upstream_conf_t         upstream;
	ngx_int_t                        key;
	ngx_int_t                        flags;
	ngx_int_t                        exptime;
	ngx_int_t                        unique;
	ngx_uint_t                       gzip_flag;
	ngx_uint_t                       value_flag;
} ngx_http_memcached_loc_conf_t;


typedef struct {
	ngx_http_request_t              *request;
	ngx_str_t                        cmd;   /* command of memcache */
	ngx_int_t                        type;  /* type of command */
	ngx_str_t                        key;   /* key of command */
} ngx_http_memcached_ctx_t;


static ngx_int_t ngx_http_memcached_create_request(ngx_http_request_t *r);
static ngx_int_t ngx_http_memcached_reinit_request(ngx_http_request_t *r);
static ngx_int_t ngx_http_memcached_process_header(ngx_http_request_t *r);
static ngx_int_t ngx_http_memcached_process_value(ngx_http_request_t *r);
static ngx_int_t ngx_http_memcached_filter_init(void *data);
static ngx_int_t ngx_http_memcached_filter(void *data, ssize_t bytes);
static void ngx_http_memcached_abort_request(ngx_http_request_t *r);
static void ngx_http_memcached_finalize_request(ngx_http_request_t *r,
	ngx_int_t rc);

static void *ngx_http_memcached_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_memcached_merge_loc_conf(ngx_conf_t *cf,
	void *parent, void *child);

static char *ngx_http_memcached_pass(ngx_conf_t *cf, ngx_command_t *cmd,
	void *conf);


static ngx_conf_bitmask_t ngx_http_memcached_next_upstream_masks[] = {
	{ ngx_string("error"), NGX_HTTP_UPSTREAM_FT_ERROR },
	{ ngx_string("timeout"), NGX_HTTP_UPSTREAM_FT_TIMEOUT },
	{ ngx_string("invalid_response"), NGX_HTTP_UPSTREAM_FT_INVALID_HEADER },
	{ ngx_string("not_found"), NGX_HTTP_UPSTREAM_FT_HTTP_404 },
	{ ngx_string("off"), NGX_HTTP_UPSTREAM_FT_OFF },
	{ ngx_null_string, 0 }
};


static ngx_command_t ngx_http_memcached_commands[] = {

	{ ngx_string("memcached_pass"),
	NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF|NGX_CONF_TAKE1,
	ngx_http_memcached_pass,
	NGX_HTTP_LOC_CONF_OFFSET,
	0,
	NULL },

	{ ngx_string("memcached_bind"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	ngx_http_upstream_bind_set_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_memcached_loc_conf_t, upstream.local),
	NULL },

	{ ngx_string("memcached_connect_timeout"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	ngx_conf_set_msec_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_memcached_loc_conf_t, upstream.connect_timeout),
	NULL },

	{ ngx_string("memcached_send_timeout"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	ngx_conf_set_msec_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_memcached_loc_conf_t, upstream.send_timeout),
	NULL },

	{ ngx_string("memcached_buffer_size"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	ngx_conf_set_size_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_memcached_loc_conf_t, upstream.buffer_size),
	NULL },

	{ ngx_string("memcached_read_timeout"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	ngx_conf_set_msec_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_memcached_loc_conf_t, upstream.read_timeout),
	NULL },

	{ ngx_string("memcached_next_upstream"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_1MORE,
	ngx_conf_set_bitmask_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_memcached_loc_conf_t, upstream.next_upstream),
	&ngx_http_memcached_next_upstream_masks },

	{ ngx_string("memcached_gzip_flag"),
	NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	ngx_conf_set_num_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_memcached_loc_conf_t, gzip_flag),
	NULL },

	{ ngx_string("memcached_value_flag"),
	NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF|NGX_CONF_TAKE1,
	ngx_conf_set_num_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_memcached_loc_conf_t, value_flag),
	NULL },

	ngx_null_command
};


static ngx_http_module_t ngx_http_memcached_module_ctx = {
	NULL,                               /* preconfiguration */
	NULL,                               /* postconfiguration */

	NULL,                               /* create main configuration */
	NULL,                               /* init main configuration */

	NULL,                               /* create server configuration */
	NULL,                               /* merge server configuration */

	ngx_http_memcached_create_loc_conf, /* create location configuration */
	ngx_http_memcached_merge_loc_conf   /* merge location configuration */
};


ngx_module_t ngx_http_memcached_module = {
	NGX_MODULE_V1,
	&ngx_http_memcached_module_ctx,     /* module context */
	ngx_http_memcached_commands,        /* module directives */
	NGX_HTTP_MODULE,                    /* module type */
	NULL,                               /* init master */
	NULL,                               /* init module */
	NULL,                               /* init process */
	NULL,                               /* init thread */
	NULL,                               /* exit thread */
	NULL,                               /* exit process */
	NULL,                               /* exit master */
	NGX_MODULE_V1_PADDING
};


static ngx_str_t ngx_http_memcached_key = ngx_string("memcached_key");
static ngx_str_t ngx_http_memcached_flags = ngx_string("memcached_flags");
static ngx_str_t ngx_http_memcached_exptime = ngx_string("memcached_exptime");
static ngx_str_t ngx_http_memcached_unique = ngx_string("memcached_unique");


#define NGX_HTTP_MEMCACHED_END      (sizeof(ngx_http_memcached_end) - 1)
static u_char ngx_http_memcached_end[] = CRLF "END" CRLF;
#define NGX_HTTP_MEMCACHED_GET      1
#define NGX_HTTP_MEMCACHED_SET      2
#define NGX_HTTP_MEMCACHED_DEL      3
#define NGX_HTTP_MEMCACHED_CAS      4


static ngx_int_t
ngx_http_memcached_handler(ngx_http_request_t *r) {
	ngx_int_t                        rc;
	ngx_http_upstream_t             *u;
	ngx_http_memcached_ctx_t        *ctx;
	ngx_http_memcached_loc_conf_t   *mlcf;
	u_char                          *cmd;
	ngx_int_t                        type;

	/* GET for get/delete */
	/* POST for set/add/replace */
	if (r->method & (NGX_HTTP_GET | NGX_HTTP_HEAD)) {
		if (NGX_OK != (rc = ngx_http_discard_request_body(r)))
			return rc;
	} else if (!(r->method & NGX_HTTP_POST)) return NGX_HTTP_NOT_ALLOWED;

	/* last unit of uri is command of memcache */
	for (cmd = r->uri.data + r->uri.len; r->uri.data < cmd;)
		if (*--cmd == '/') break;
	if (cmd == r->uri.data) return NGX_HTTP_BAD_REQUEST;
	++cmd;
	if (!(ngx_memcmp(cmd, "get", sizeof("get") - 1)
		&& ngx_memcmp(cmd, "gets", sizeof("gets") - 1)))
		type = NGX_HTTP_MEMCACHED_GET;
	else if (!(ngx_memcmp(cmd, "set", sizeof("set") - 1)
		&& ngx_memcmp(cmd, "add", sizeof("add") - 1)
		&& ngx_memcmp(cmd, "replace", sizeof("replace") - 1)))
		type = NGX_HTTP_MEMCACHED_SET;
	else if (!ngx_memcmp(cmd, "delete", sizeof("delete") - 1))
		type = NGX_HTTP_MEMCACHED_DEL;
	else if (!ngx_memcmp(cmd, "cas", sizeof("cas") - 1))
		type = NGX_HTTP_MEMCACHED_CAS;
	else return NGX_HTTP_BAD_REQUEST;

	if (NGX_OK != ngx_http_set_content_type(r))
		return NGX_HTTP_INTERNAL_SERVER_ERROR;

	if (NGX_OK != ngx_http_upstream_create(r))
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	u = r->upstream;

	ngx_str_set(&u->schema, "memcached://");
	u->output.tag = (ngx_buf_tag_t)&ngx_http_memcached_module;

	mlcf = ngx_http_get_module_loc_conf(r, ngx_http_memcached_module);
	u->conf = &mlcf->upstream;

	u->create_request = ngx_http_memcached_create_request;
	u->reinit_request = ngx_http_memcached_reinit_request;
	u->process_header = ngx_http_memcached_process_header;
	u->abort_request = ngx_http_memcached_abort_request;
	u->finalize_request = ngx_http_memcached_finalize_request;

	/* context */
	if (!(ctx = ngx_palloc(r->pool, sizeof(ngx_http_memcached_ctx_t))))
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	ctx->request = r;
	ctx->cmd.data = cmd;
	ctx->cmd.len = r->uri.len - (ctx->cmd.data - r->uri.data);
	ctx->type = type;
	ngx_http_set_ctx(r, ctx, ngx_http_memcached_module);

	u->input_filter_init = ngx_http_memcached_filter_init;
	u->input_filter = ngx_http_memcached_filter;
	u->input_filter_ctx = ctx;

	if (r->method & NGX_HTTP_POST) {
		/* POST */
		if (NGX_HTTP_SPECIAL_RESPONSE <= (rc =
			ngx_http_read_client_request_body(r, ngx_http_upstream_init)))
			return rc;
	} else {
		/* GET */
		++r->main->count;
		ngx_http_upstream_init(r);
	}
	return NGX_DONE;
}


static ngx_int_t
ngx_http_memcached_create_request(ngx_http_request_t *r) {
	size_t                           len;
	uintptr_t                        escape;
	ngx_buf_t                       *b;
	ngx_chain_t                     *cl;
	ngx_http_memcached_ctx_t        *ctx;
	ngx_http_variable_value_t       *vv[4];
	ngx_http_memcached_loc_conf_t   *mlcf;

	mlcf = ngx_http_get_module_loc_conf(r, ngx_http_memcached_module);
	ctx = ngx_http_get_module_ctx(r, ngx_http_memcached_module);

	if (!ctx || !ctx->type) return NGX_ERROR;

	len = ctx->cmd.len;

	/* key of command */
	if (!(vv[0] = ngx_http_get_indexed_variable(r, mlcf->key))
		|| vv[0]->not_found || !vv[0]->len) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"the \"$memcached_key\" variable is not set");
		return NGX_ERROR;
	}
	escape = 2 * ngx_escape_uri(NULL, vv[0]->data, vv[0]->len,
		NGX_ESCAPE_MEMCACHED);
	len += 1 + vv[0]->len + escape;

	vv[3] = vv[2] = vv[1] = NULL;
	if (NGX_HTTP_MEMCACHED_SET == ctx->type
		|| NGX_HTTP_MEMCACHED_CAS == ctx->type) {
		if (r->method & NGX_HTTP_POST &&
			r->headers_in.content_length_n < 0) return NGX_ERROR;

		/* flags of command */
		++len;
		if ((vv[1] = ngx_http_get_indexed_variable(r, mlcf->flags))
			&& !vv[1]->not_found && vv[1]->len) {
			len += vv[1]->len;
		} else ++len;
		/* expiration time of command */
		++len;
		if ((vv[2] = ngx_http_get_indexed_variable(r, mlcf->exptime))
			&& !vv[2]->not_found && vv[2]->len) {
			len += vv[2]->len;
		} else ++len;
		len += 1 + (r->headers_in.content_length_n < 0
			? 1 : r->headers_in.content_length->value.len);
		if (NGX_HTTP_MEMCACHED_CAS == ctx->type) {
			/* cas unqiue of command */
			++len;
			if (!(vv[3] = ngx_http_get_indexed_variable(r, mlcf->unique))
				|| vv[3]->not_found || !vv[3]->len) {
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					"the \"$memcached_unique\" variable is not set");
				return NGX_ERROR;
			}
			len += vv[3]->len;
		}
	}

	len += sizeof(CRLF) - 1;
	len += sizeof(CRLF) - 1;

	/* buf for command */
	if (!(b = ngx_create_temp_buf(r->pool, len))) return NGX_ERROR;
	if (!(cl = ngx_alloc_chain_link(r->pool))) return NGX_ERROR;
	cl->buf = b;
	cl->next = NULL;
	r->upstream->request_bufs = cl;

	/* <command> */
	b->last = ngx_copy(b->last, ctx->cmd.data, ctx->cmd.len);

	*b->last++ = ' ';
	/* <key> */
	ctx->key.data = b->last;
	if (escape)
		b->last = (u_char *)ngx_escape_uri(b->last, vv[0]->data, vv[0]->len,
			NGX_ESCAPE_MEMCACHED);
	else
		b->last = ngx_copy(b->last, vv[0]->data, vv[0]->len);
	ctx->key.len = b->last - ctx->key.data;

	/* <flags> <expiration time> <bytes> */
	if (NGX_HTTP_MEMCACHED_SET == ctx->type
		|| NGX_HTTP_MEMCACHED_CAS == ctx->type) {
		*b->last++ = ' ';
		/* flags */
		if (vv[1] && !vv[1]->not_found && vv[1]->len)
			b->last = ngx_copy(b->last, vv[1]->data, vv[1]->len);
		else *b->last++ = '0';
		*b->last++ = ' ';
		/* expiration time */
		if (vv[2] && !vv[2]->not_found && vv[2]->len)
			b->last = ngx_copy(b->last, vv[2]->data, vv[2]->len);
		else *b->last++ = '0';
		*b->last++ = ' ';
		/* bytes */
		if (r->headers_in.content_length_n < 0)
			*b->last++ = '0';
		else b->last = ngx_copy(b->last,
			r->headers_in.content_length->value.data,
			r->headers_in.content_length->value.len);
		if (NGX_HTTP_MEMCACHED_CAS == ctx->type) {
			*b->last++ = ' ';
			/* unique */
			b->last = ngx_copy(b->last, vv[3]->data, vv[3]->len);
		}
		/* CRLF */
		*b->last++ = CR, *b->last++ = LF;
	}

	if (r->request_body && r->request_body->bufs) {
		for (cl = cl->next = r->request_body->bufs; cl->next; cl = cl->next);
		cl->buf->last_buf = 0;

		if (!(b = ngx_create_temp_buf(r->pool, len))) return NGX_ERROR;
		if (!(cl->next = ngx_alloc_chain_link(r->pool))) return NGX_ERROR;
		cl = cl->next;
		cl->buf = b;
		cl->next = NULL;
	}

	/* CRLF */
	*b->last++ = CR, *b->last++ = LF;
	b->last_buf = 1;

	/*
     * Summary, the request looks like this:
	 * get $memcached_key\r\n
	 *
	 * set $memcached_key $memcached_flags $memcached_exptime bytes\r\n
	 * add $memcached_key $memcached_flags $memcached_exptime bytes\r\n
	 * replace $memcached_key $memcached_flags $memcached_exptime bytes\r\n
	 * cas $memcached_key $memcached_flags $memcached_exptime bytes unique\r\n
	 * <...>\r\n
	 *
	 * delete $memcached_key\r\n
     * where $memcached_key, $memcached_flags, $memcached_exptime
	 * and $memcached_unique are variable's values.
     */

	ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
		"http memcached request: %V \"%V\"", &ctx->cmd, &ctx->key);

	return NGX_OK;
}


static ngx_int_t
ngx_http_memcached_reinit_request(ngx_http_request_t *r) {
	return NGX_OK;
}


#define NGX_HTTP_PREPARE_RESPONSE_HEADER(u, st) \
	u->headers_in.content_length_n = 0; \
	u->headers_in.status_n = u->state->status = 200; \
	ngx_str_set(&u->headers_in.status_line, st); \
	u->keepalive = 1


static ngx_int_t
ngx_http_memcached_process_header(ngx_http_request_t *r) {
	u_char                          *p, *start;
	ngx_str_t                        line;
	ngx_uint_t                       flags;
	ngx_table_elt_t                 *h;
	ngx_http_upstream_t             *u;
	ngx_http_memcached_ctx_t        *ctx;
	ngx_http_memcached_loc_conf_t   *mlcf;

	for (u = r->upstream, p = u->buffer.pos; p < u->buffer.last; ++p) {
		if (*p == LF) goto found;
	}
	return NGX_AGAIN;

found:

	line.data = u->buffer.pos;
	u->buffer.pos = p + 1; /* eat */
	if (*--p != CR) goto no_valid;
	*p = '\0';
	line.len = p - line.data;

	ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
		"memcached: \"%V\"", &line);

	ctx = ngx_http_get_module_ctx(r, ngx_http_memcached_module);
	mlcf = ngx_http_get_module_loc_conf(r, ngx_http_memcached_module);

	p = line.data;
	switch (ctx->type) {
	case NGX_HTTP_MEMCACHED_SET:
	case NGX_HTTP_MEMCACHED_DEL:
	case NGX_HTTP_MEMCACHED_CAS:
		if (!ngx_strcmp(p, "STORED")) {
			ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
				"key: \"%V\" was stored by memcached", &ctx->key);

			NGX_HTTP_PREPARE_RESPONSE_HEADER(u, "200 STORED");
			return NGX_OK;
		}
		if (!ngx_strcmp(p, "NOT_STORED")) {
			ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
				"key: \"%V\" was not found by memcached", &ctx->key);

			NGX_HTTP_PREPARE_RESPONSE_HEADER(u, "404 NOT_STORED");
			return NGX_OK;
		}
		if (!ngx_strcmp(p, "NOT_FOUND")) {
			ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
				"key: \"%V\" was not found by memcached", &ctx->key);

			NGX_HTTP_PREPARE_RESPONSE_HEADER(u, "404 NOT_FOUND");
			return NGX_OK;
		}
		if (!ngx_strcmp(p, "DELETED")) {
			ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
				"key: \"%V\" was deleted by memcached", &ctx->key);

			NGX_HTTP_PREPARE_RESPONSE_HEADER(u, "200 DELETED");
			return NGX_OK;
		}
		if (!ngx_strcmp(p, "EXISTS")) {
			ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
				"key: \"%V\" was not found by memcached", &ctx->key);

			NGX_HTTP_PREPARE_RESPONSE_HEADER(u, "406 EXISTS");
			return NGX_OK;
		}
		break;
	case NGX_HTTP_MEMCACHED_GET:
		/* VALUE <key> <flags> <bytes> */
		if (!ngx_strncmp(p, "VALUE ", sizeof("VALUE ") - 1)) {
			p += sizeof("VALUE ") - 1;

			if (ngx_strncmp(p, ctx->key.data, ctx->key.len)) {
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					"memcached sent invalid key in response \"%V\" "
					"for key \"%V\"",
					&line, &ctx->key);

				return NGX_HTTP_UPSTREAM_INVALID_HEADER;
			}

			if (*(p += ctx->key.len) != ' ') goto no_valid;

			/* flags */
			for (start = ++p; *p;) {
				if (*p++ == ' ') {
					/*if (mlcf->gzip_flag)*/ goto flags;
					/*else goto length;*/
				}
			}
			goto no_valid;

		flags:
			if (!(h = ngx_list_push(&r->headers_out.headers)))
				return NGX_ERROR;
			h->hash = 1;
			ngx_str_set(&h->key, "Memcached-Flags");
			h->value.data = start;
			h->value.len = p - 1 - start;

			/* gzip flags for data */
			flags = ngx_atoi(start, p - 1 - start);
			if (flags == (ngx_uint_t)NGX_ERROR) {
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					"memcached sent invalid flags in response \"%V\" "
					"for key \"%V\"",
					&line, &ctx->key);
				return NGX_HTTP_UPSTREAM_INVALID_HEADER;
			}

			if (flags & mlcf->gzip_flag) {
				if (!(h = ngx_list_push(&r->headers_out.headers)))
					return NGX_ERROR;
				h->hash = 1;
				ngx_str_set(&h->key, "Content-Encoding");
				ngx_str_set(&h->value, "gzip");

				r->headers_out.content_encoding = h;
			}

		/*length:*/
			for (start = p; *p && *p != ' '; ++p);

			u->headers_in.content_length_n = ngx_atoof(start, p - start);
			if (u->headers_in.content_length_n == -1) {
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					"memcached sent invalid length in response \"%V\" "
					"for key \"%V\"",
					&line, &ctx->key);
				return NGX_HTTP_UPSTREAM_INVALID_HEADER;
			}

		/*unique:*/
			if (*p) {
				if (!(h = ngx_list_push(&r->headers_out.headers)))
					return NGX_ERROR;
				h->hash = 1;
				ngx_str_set(&h->key, "Memcached-Unique");
				h->value.data = ++p;
				h->value.len = line.data + line.len - p;
			}

		/*value:*/
			if (mlcf->value_flag) {
				u->process_header = ngx_http_memcached_process_value;
				return ngx_http_memcached_process_value(r);
			}

			u->headers_in.status_n = u->state->status = 200;
			ngx_str_set(&u->headers_in.status_line, "200 OK");
			return NGX_OK;
		}

		if (!ngx_strcmp(p, "END")) {
			ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
				"key: \"%V\" was not found by memcached", &ctx->key);

			NGX_HTTP_PREPARE_RESPONSE_HEADER(u, "404 NOT_FOUND");
			return NGX_OK;
		}
	}
no_valid:

	ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
		"memcached sent invalid response: \"%V\"", &line);

	return NGX_HTTP_UPSTREAM_INVALID_HEADER;
}


static ngx_int_t
ngx_http_memcached_process_value(ngx_http_request_t *r) {
	ngx_table_elt_t                 *h;
	ngx_http_upstream_t             *u;

	u = r->upstream;
	if (u->buffer.last - u->buffer.pos < u->headers_in.content_length_n)
		return NGX_AGAIN;

	if (0 < u->headers_in.content_length_n) {
		if (!(h = ngx_list_push(&r->headers_out.headers)))
			return NGX_ERROR;
		h->hash = 1;
		ngx_str_set(&h->key, "Memcached-Value");
		h->value.data = u->buffer.pos;
		h->value.len = u->headers_in.content_length_n;
		u->buffer.pos += u->headers_in.content_length_n; /* eat header */
	}

	u->headers_in.content_length_n = 0;
	u->headers_in.status_n = u->state->status = 200;
	ngx_str_set(&u->headers_in.status_line, "200 OK");
	return NGX_OK;
}


static ngx_int_t
ngx_http_memcached_filter_init(void *data) {
	ngx_http_memcached_ctx_t        *ctx;
	ngx_http_upstream_t             *u;

	ctx = data;
	u = ctx->request->upstream;

	switch (ctx->type) {
	case NGX_HTTP_MEMCACHED_GET:
		/* content length will be received from upstream */
		u->length = *u->headers_in.status_line.data == '2'
			? u->headers_in.content_length_n + NGX_HTTP_MEMCACHED_END : 0;
		break;
	case NGX_HTTP_MEMCACHED_SET:
	case NGX_HTTP_MEMCACHED_DEL:
	case NGX_HTTP_MEMCACHED_CAS:
	default:
		u->length = 0;
	}

	return NGX_OK;
}


static ngx_int_t
ngx_http_memcached_filter(void *data, ssize_t bytes) {
	ngx_http_memcached_ctx_t        *ctx;
	ngx_http_upstream_t             *u;
	u_char                          *last;
	ngx_buf_t                       *b;
	ngx_chain_t                     *cl, **ll;

	ctx = data;
	u = ctx->request->upstream;
	b = &u->buffer;
	last = b->last;
	b->last += bytes;

	/* set valid data for client */
	if ((ssize_t)NGX_HTTP_MEMCACHED_END < u->length) {
		for (cl = u->out_bufs, ll = &u->out_bufs; cl; cl = cl->next)
			ll = &cl->next;

		if (!(cl = ngx_chain_get_free_buf(ctx->request->pool, &u->free_bufs)))
			return NGX_ERROR;
		cl->buf->flush = 1;
		cl->buf->memory = 1;

		*ll = cl;
		
		cl->buf->pos = last;
		last += bytes;
		/* has \r\nEND\r\n ? */
		if ((u->length -= bytes) < (ssize_t)NGX_HTTP_MEMCACHED_END) {
			last -= NGX_HTTP_MEMCACHED_END - u->length;
			u->length = NGX_HTTP_MEMCACHED_END;
		}
		cl->buf->last = last;
		cl->buf->tag = u->output.tag;

		ngx_log_debug3(NGX_LOG_DEBUG_HTTP, ctx->request->connection->log, 0,
			"memcached filter bytes:%z size:%z length:%z",
			bytes, cl->buf->last - cl->buf->pos, u->length);

		/* eat */
		bytes -= cl->buf->last - cl->buf->pos;
	}

	/* eat \r\nEND\r\n */
	if (0 < bytes) {
		if (ngx_strncmp(last, ngx_http_memcached_end + NGX_HTTP_MEMCACHED_END - u->length, bytes)) {
			ngx_log_error(NGX_LOG_ERR, ctx->request->connection->log, 0,
				"memcached sent invalid trailer");

			u->length = 0;

			return NGX_OK;
		}

		if (!(u->length -= bytes)) u->keepalive = 1;
	}

	return NGX_OK;
}


static void
ngx_http_memcached_abort_request(ngx_http_request_t *r) {
	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
		"abort http memcached request");
	return;
}


static void
ngx_http_memcached_finalize_request(ngx_http_request_t *r, ngx_int_t rc) {
	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
		"finalize http memcached request");
	return;
}


static void *
ngx_http_memcached_create_loc_conf(ngx_conf_t *cf) {
	ngx_http_memcached_loc_conf_t   *conf;

	if (!(conf = ngx_pcalloc(cf->pool,
		sizeof(ngx_http_memcached_loc_conf_t)))) return NULL;

	/*
	 * set by ngx_pcalloc():
	 *
	 *     conf->upstream.bufs.num = 0;
	 *     conf->upstream.next_upstream = 0;
	 *     conf->upstream.temp_path = NULL;
	 *     conf->upstream.uri = { 0, NULL };
	 *     conf->upstream.location = NULL;
	 */

	conf->upstream.local = NGX_CONF_UNSET_PTR;
	conf->upstream.connect_timeout = NGX_CONF_UNSET_MSEC;
	conf->upstream.send_timeout = NGX_CONF_UNSET_MSEC;
	conf->upstream.read_timeout = NGX_CONF_UNSET_MSEC;

	conf->upstream.buffer_size = NGX_CONF_UNSET_SIZE;

	/* the hardcoded values */
	conf->upstream.cyclic_temp_file = 0;
	conf->upstream.buffering = 0;
	conf->upstream.ignore_client_abort = 0;
	conf->upstream.send_lowat = 0;
	conf->upstream.bufs.num = 0;
	conf->upstream.busy_buffers_size = 0;
	conf->upstream.max_temp_file_size = 0;
	conf->upstream.temp_file_write_size = 0;
	conf->upstream.intercept_errors = 1;
	conf->upstream.intercept_404 = 1;
	conf->upstream.pass_request_headers = 0;
	conf->upstream.pass_request_body = 0;

	/* customer variable */
	conf->key = NGX_CONF_UNSET;
	conf->flags = NGX_CONF_UNSET;
	conf->exptime = NGX_CONF_UNSET;
	conf->unique = NGX_CONF_UNSET;

	conf->gzip_flag = NGX_CONF_UNSET_UINT;
	conf->value_flag = NGX_CONF_UNSET_UINT;

	return conf;
}


static char *
ngx_http_memcached_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child) {
	ngx_http_memcached_loc_conf_t   *prev = parent;
	ngx_http_memcached_loc_conf_t   *conf = child;

	ngx_conf_merge_ptr_value(conf->upstream.local,
		prev->upstream.local, NULL);

	ngx_conf_merge_msec_value(conf->upstream.connect_timeout,
		prev->upstream.connect_timeout, 60000);

	ngx_conf_merge_msec_value(conf->upstream.send_timeout,
		prev->upstream.send_timeout, 60000);

	ngx_conf_merge_msec_value(conf->upstream.read_timeout,
		prev->upstream.read_timeout, 60000);

	ngx_conf_merge_size_value(conf->upstream.buffer_size,
		prev->upstream.buffer_size,
		(size_t) ngx_pagesize);

	ngx_conf_merge_bitmask_value(conf->upstream.next_upstream,
		prev->upstream.next_upstream,
		NGX_CONF_BITMASK_SET
			| NGX_HTTP_UPSTREAM_FT_ERROR
			| NGX_HTTP_UPSTREAM_FT_TIMEOUT);

	if (conf->upstream.next_upstream & NGX_HTTP_UPSTREAM_FT_OFF)
		conf->upstream.next_upstream = NGX_CONF_BITMASK_SET
			| NGX_HTTP_UPSTREAM_FT_OFF;

	if (!conf->upstream.upstream)
		conf->upstream.upstream = prev->upstream.upstream;

	/* customer variable */
	if (conf->key == NGX_CONF_UNSET) conf->key = prev->key;
	if (conf->flags == NGX_CONF_UNSET) conf->flags = prev->flags;
	if (conf->exptime == NGX_CONF_UNSET) conf->exptime = prev->exptime;
	if (conf->unique == NGX_CONF_UNSET) conf->unique = prev->unique;

	ngx_conf_merge_uint_value(conf->gzip_flag, prev->gzip_flag, 0);
	ngx_conf_merge_uint_value(conf->value_flag, prev->value_flag, 0);

	return NGX_CONF_OK;
}


static char *
ngx_http_memcached_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
	ngx_http_memcached_loc_conf_t   *mlcf;
	ngx_http_core_loc_conf_t        *clcf;
	ngx_str_t                       *value;
	ngx_url_t                        url;

	mlcf = conf;
	if (mlcf->upstream.upstream) return "is duplicate";

	value = cf->args->elts;

	ngx_memzero(&url, sizeof(ngx_url_t));
	url.url = value[1];
	url.no_resolve = 1;
	if (!(mlcf->upstream.upstream = ngx_http_upstream_add(cf, &url, 0)))
		return NGX_CONF_ERROR;

	clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
	clcf->handler = ngx_http_memcached_handler;

	if (clcf->name.data[clcf->name.len - 1] == '/')
		clcf->auto_redirect = 1;

	/* define customer variable */
	if (NGX_ERROR == (mlcf->key = ngx_http_get_variable_index(cf,
		&ngx_http_memcached_key)))
		return NGX_CONF_ERROR;
	if (NGX_ERROR == (mlcf->flags = ngx_http_get_variable_index(cf,
		&ngx_http_memcached_flags)))
		return NGX_CONF_ERROR;
	if (NGX_ERROR == (mlcf->exptime = ngx_http_get_variable_index(cf,
		&ngx_http_memcached_exptime)))
		return NGX_CONF_ERROR;
	if (NGX_ERROR == (mlcf->unique = ngx_http_get_variable_index(cf,
		&ngx_http_memcached_unique)))
		return NGX_CONF_ERROR;

	return NGX_CONF_OK;
}
