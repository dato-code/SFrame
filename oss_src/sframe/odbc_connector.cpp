/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sframe/odbc_connector.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#define INFO_LEVEL LOG_INFO

#if (ODBCVER >= 0x0300)
#define SQL_IS_INTERVAL(type) (type >= SQL_INTERVAL_YEAR && type <= SQL_INTERVAL_MINUTE_TO_SECOND)
#define SQL_C_IS_INTERVAL(type) (type >= SQL_C_INTERVAL_YEAR && type <= SQL_C_INTERVAL_MINUTE_TO_SECOND)
#else
#define SQL_IS_INTERVAL(type) (type <= SQL_INTERVAL_YEAR && type >= SQL_INTERVAL_MINUTE_TO_SECOND)
#define SQL_C_IS_INTERVAL(type) (type <= SQL_C_INTERVAL_YEAR && type >= SQL_C_INTERVAL_MINUTE_TO_SECOND)
#endif
// This means "varying" in the SQL sense. VARCHAR is varying and CHAR is fixed
// on instantiation of a SQL type
#define SQL_IS_VARYING(type) ((type == SQL_VARCHAR) || \
                              (type == SQL_LONGVARCHAR) || \
                              (type == SQL_WVARCHAR) || \
                              (type == SQL_WLONGVARCHAR) || \
                              (type == SQL_VARBINARY) || \
                              (type == SQL_LONGVARBINARY))

// This means "variable length" from a C types sense. So CHAR is variable
// length because it could be any length of string of characters, whereas
// INTEGER is always 4 bytes, for example.
#define SQL_IS_VARIABLE_LENGTH(type) ((SQL_IS_VARYING(type)) || \
                                      (type == SQL_CHAR) || \
                                      (type == SQL_BINARY) || \
                                      (type == SQL_WCHAR))


namespace graphlab {

static SQLCHAR g_sql_state[7];
// The SQL state code to indicate that the data returned has been right
// truncated. SQLCHAR is an unsigned char, so I have to declare it this way.
const static SQLCHAR truncated_text_state[7] = {'0','1','0','0','4','\0','\0'};

/**
 * Convert an ODBC SQL type to flex and C types
 */
std::pair<flex_type_enum, SQLSMALLINT> odbc_type_to_flex(SQLSMALLINT odbc_type) {
  switch(odbc_type) {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
    case SQL_GUID:
      return std::make_pair(flex_type_enum::STRING, SQL_C_CHAR);
    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
      return std::make_pair(flex_type_enum::STRING, SQL_C_BINARY);
    case SQL_DECIMAL:
    case SQL_NUMERIC:
    case SQL_REAL:
    case SQL_FLOAT:
    case SQL_DOUBLE:
      return std::make_pair(flex_type_enum::FLOAT, SQL_C_DOUBLE);
    case SQL_SMALLINT:
    case SQL_INTEGER:
    case SQL_BIT:
    case SQL_TINYINT:
    case SQL_BIGINT:
      return std::make_pair(flex_type_enum::INTEGER, SQL_C_SBIGINT);
    case SQL_TYPE_DATE:
      return std::make_pair(flex_type_enum::DATETIME, SQL_C_TYPE_DATE);
    case SQL_TYPE_TIME:
      return std::make_pair(flex_type_enum::DATETIME, SQL_C_TYPE_TIME);
    case SQL_TYPE_TIMESTAMP:
      return std::make_pair(flex_type_enum::DATETIME, SQL_C_TYPE_TIMESTAMP);
    case SQL_INTERVAL_YEAR:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_YEAR);
    case SQL_INTERVAL_MONTH:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_MONTH);
    case SQL_INTERVAL_DAY:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_DAY);
    case SQL_INTERVAL_HOUR:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_HOUR);
    case SQL_INTERVAL_MINUTE:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_MINUTE);
    case SQL_INTERVAL_SECOND:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_SECOND);
    case SQL_INTERVAL_YEAR_TO_MONTH:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_YEAR_TO_MONTH);
    case SQL_INTERVAL_DAY_TO_HOUR:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_DAY_TO_HOUR);
    case SQL_INTERVAL_DAY_TO_MINUTE:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_DAY_TO_MINUTE);
    case SQL_INTERVAL_DAY_TO_SECOND:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_DAY_TO_SECOND);
    case SQL_INTERVAL_HOUR_TO_MINUTE:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_HOUR_TO_MINUTE);
    case SQL_INTERVAL_HOUR_TO_SECOND:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_HOUR_TO_SECOND);
    case SQL_INTERVAL_MINUTE_TO_SECOND:
      return std::make_pair(flex_type_enum::DICT, SQL_C_INTERVAL_MINUTE_TO_SECOND);
    default:
      return std::make_pair(flex_type_enum::UNDEFINED, SQL_C_CHAR);
  }
}

void odbc_connector::handle_return(SQLRETURN orig_ret_status,
                   const char *fn,
                   SQLHANDLE handle,
                   SQLSMALLINT type,
                   std::string user_err_msg) {
  SQLINTEGER   i = 0;
  SQLINTEGER   native;
  //SQLCHAR  state[ 7 ];
  SQLCHAR  text[256];
  SQLSMALLINT  len;
  SQLRETURN  ret;

  int log_lvl = LOG_ERROR;
  std::stringstream odbc_err_msg;

  if(SQL_SUCCEEDED(orig_ret_status) || (orig_ret_status == SQL_NO_DATA)) {
    if(orig_ret_status == SQL_SUCCESS_WITH_INFO) {
      log_lvl = INFO_LEVEL;
    } else {
      return;
    }
  }

  if(log_lvl == LOG_ERROR) {

    logstream(LOG_ERROR) << fn << " diagnostics: " << std::endl;
  } else {
    //logstream(INFO_LEVEL) << fn << " diagnostics: " << std::endl;
  }

  int count = 0;
  do
  {
    //TODO: One too many calls to SQLGetDiagRec causes the process to crash when
    //using the SQLite driver.  Therefore I limit this to just one error message,
    //though there could be many.
    ret = SQLGetDiagRec(type, handle, ++i, g_sql_state, &native, text,
                          sizeof(text), &len );
    if (SQL_SUCCEEDED(ret)) {
      odbc_err_msg << g_sql_state << ":" << i << ":" << native << ":" << text;
      if(log_lvl == LOG_ERROR) {
        //logstream(LOG_ERROR) << odbc_err_msg.str() << std::endl;
        odbc_err_msg << std::endl;
        log_and_throw(user_err_msg + std::string("\n") + odbc_err_msg.str());
      } else {
        // TOO much logging
        //logstream(INFO_LEVEL) << odbc_err_msg.str() << std::endl;
      }
    }
    ++count;
  }
  //while( ret == SQL_SUCCESS );
  while(0);

  if(log_lvl == LOG_ERROR) {
    if(user_err_msg.length() == 0) {
      user_err_msg = std::string(fn);
    }
    if(m_query_running)
      this->finalize_query();
    log_and_throw(user_err_msg);
  }
}

odbc_connector::odbc_connector() : m_inited(false), m_query_running(false),
  m_types_mapped(false), m_entry_buffer(NULL), m_entry_buffer_size(NULL),
  m_num_rows_to_fetch(0), m_name_buf(NULL), m_dbms_info_available(0),
  m_row_bound_params(NULL), m_value_size_indicator(NULL) {

  log_func_entry();
}

odbc_connector::~odbc_connector() {
  log_func_entry();

  this->clear();
}

void odbc_connector::clear() {
  log_func_entry();
  if(m_query_stmt)
    SQLFreeHandle(SQL_HANDLE_STMT, m_query_stmt);

  SQLDisconnect(m_dbc);

  if(m_dbc)
    SQLFreeHandle(SQL_HANDLE_DBC, m_dbc);
  if(m_env)
    SQLFreeHandle(SQL_HANDLE_ENV, m_env);

  if(m_name_buf) {
    free(m_name_buf);
    m_name_buf = NULL;
  }
}

void odbc_connector::init(const std::string &conn_str) {
  log_func_entry();

  // Set up environment.  We want ODBC 3 behavior
  m_ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_env);
  if(m_ret == SQL_ERROR) {
    log_and_throw("Unable to allocate ODBC environment handle!\nThis is "
        "probably because GraphLab Create cannot load libodbc.{so,dylib}.\n"
        "Have you installed an ODBC driver manager? If you have, set the\n"
        "directory to the GRAPHLAB_LIBODBC_PREFIX environment variable and\n"
        "restart GraphLab Create.\n");
  }
  m_ret = SQLSetEnvAttr(m_env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
  handle_return(m_ret, "SQLSetEnvAttr", m_env, SQL_HANDLE_ENV,
      "Failed to declare ODBC driver behavior.");

  // Set up connection
  m_ret = SQLAllocHandle(SQL_HANDLE_DBC, m_env, &m_dbc);
  handle_return(m_ret, "SQLAllocHandle", m_env, SQL_HANDLE_ENV,
      "Failed to allocate DB connection");

  m_ret = SQLDriverConnect(m_dbc,
                         NULL,
                         (SQLCHAR *)conn_str.c_str(),
                         SQL_NTS,
                         NULL,
                         0,
                         NULL,
                         SQL_DRIVER_COMPLETE);
  handle_return(m_ret, "SQLDriverConnect", m_dbc, SQL_HANDLE_DBC,
      "Failed to connect to ODBC driver.");

  // Allocate our statement for reading
  m_ret = SQLAllocHandle(SQL_HANDLE_STMT, m_dbc, &m_query_stmt);
  handle_return(m_ret, "SQLAllocHandle", m_dbc, SQL_HANDLE_DBC, "Failed to allocate statement object");

  if(SQL_SUCCEEDED(m_ret)) {
    m_inited = true;
  }

  // Get various connection-specific information
  m_ret = SQLGetInfo(m_dbc, SQL_MAX_COLUMN_NAME_LEN, &m_name_buf_len, sizeof(m_name_buf_len), NULL);
  handle_return(m_ret, "SQLGetInfo", m_dbc, SQL_HANDLE_DBC,
      "Failed to get max column name length");

  m_ret = SQLGetInfo(m_dbc, SQL_IDENTIFIER_QUOTE_CHAR, m_identifier_quote_char, 2, NULL);
  handle_return(m_ret, "SQLGetInfo", m_dbc, SQL_HANDLE_DBC,
      "Failed to get DB-specific quote character");
  // We assume that this is actually just a single character.  We do this
  // because SQLite ODBC driver has a bug where it doesn't return a
  // NULL-terminated string if the string is one character.
  m_identifier_quote_char[1] = (SQLCHAR)0;

  // If these calls fail, it's not really worth doing anything about it
  m_ret = SQLGetInfo(m_dbc, SQL_DBMS_NAME, m_dbms_name, sizeof(m_dbms_name), NULL);
  m_dbms_info_available |= m_ret;

  m_ret = SQLGetInfo(m_dbc, SQL_DBMS_VER, m_dbms_ver, sizeof(m_dbms_ver), NULL);
  m_dbms_info_available |= m_ret;

  m_name_buf = (SQLCHAR *)malloc(m_name_buf_len);

  // Get the supported types for this database
  m_ret = SQLGetTypeInfo(m_query_stmt, SQL_ALL_TYPES);
  handle_return(m_ret, "SQLGetTypeInfo", m_query_stmt, SQL_HANDLE_STMT, "Failed to get type info");

  auto ret_cols = this->start_query(std::string(""));
  if(ret_cols < 1) {
    //TODO: In reality, this should probably just disable writing to the DB,
    //but we don't have a way of doing that yet
    log_and_throw("No DB-specific type info found. Cannot proceed.");
  }

  auto type_info_names = this->get_column_names();

  size_t count = 0;
  std::stringstream strm;
  for(auto it = type_info_names.begin(); it != type_info_names.end(); ++it, ++count) {
    m_db_type_info_names.insert(std::make_pair(*it, count));
    strm << *it << ",";
  }
  logstream(LOG_INFO) << strm.str() << std::endl;

  size_t row_count = 0;
  auto row_block = this->get_query_block();
  while(row_block.size() > 0) {
    // Iterate through each row in block
    for(auto it = row_block.begin(); it != row_block.end(); ++it, ++row_count) {
      // Add the entire row 
      m_db_type_info.push_back(*it);

      // Keep track of which row corresponds to which ODBC SQL type
      auto ins_ret = m_db_type_info_by_sql_type.insert(std::make_pair(it->at(1),
            std::vector<size_t>(1, row_count)));
      if(!ins_ret.second) {
        ins_ret.first->second.push_back(row_count);
      }

    }
    row_block = this->get_query_block();
  }

  for(auto it = m_db_type_info.begin(); it != m_db_type_info.end(); ++it) {
    std::stringstream ss;
    for(auto row_it = it->begin(); row_it != it->end(); ++row_it) {
      ss << *row_it << ",";
    }
    logstream(LOG_INFO) << ss.str() << std::endl;
  }

  this->map_types_for_writing();
}

void odbc_connector::finalize_query() {
  log_func_entry();

  if(!m_query_running || !m_inited) {
    log_and_throw("Cannot finalize a query that isn't running!");
  }

  // Unbind all columns if we used SQLBindCol
  if(!m_large_columns.size()) {
    m_ret = SQLFreeStmt(m_query_stmt, SQL_UNBIND);
    handle_return(m_ret, "SQLFreeStmt", m_query_stmt, SQL_HANDLE_STMT,
        "Could not unbind columns!");
  }

  m_result_column_info.clear();
  m_large_columns.clear();
  m_total_allocated_for_read = 0;

  m_query_running = false;

  m_ret = SQLCloseCursor(m_query_stmt);
  handle_return(m_ret, "SQLCloseCursor", m_query_stmt, SQL_HANDLE_STMT,
      "Could not close cursor on query!");

  // Release all allocated memory in the m_entry_buffer
  if(m_entry_buffer != NULL) {
    for(SQLSMALLINT i = 0; i < m_num_result_cols; ++i) {
      if(m_entry_buffer[i] != NULL)
        free(m_entry_buffer[i]);
    }
    free(m_entry_buffer);
    m_entry_buffer = NULL;
  }

  if(m_entry_buffer_size != NULL) {
    for(SQLSMALLINT i = 0; i < m_num_result_cols; ++i) {
      if(m_entry_buffer_size[i] != NULL)
        free(m_entry_buffer_size[i]);
    }
    free(m_entry_buffer_size);
    m_entry_buffer_size = NULL;
  }

  if(m_large_column_buffer != NULL) {
    free(m_large_column_buffer);
    m_large_column_buffer = NULL;
  }

  m_large_column_buffer_size = 0;

  m_num_result_cols = 0;

}

size_t odbc_connector::calculate_batch_size(size_t max_row_size) {
  auto tmp_num_rows = graphlab::ODBC_BUFFER_SIZE / max_row_size;

  tmp_num_rows = std::min(tmp_num_rows, graphlab::ODBC_BUFFER_MAX_ROWS);

  logstream(LOG_INFO) << "Batch size: " << tmp_num_rows << std::endl;
  
  return tmp_num_rows;
}

size_t odbc_connector::start_query(const std::string &query_str) {
  log_func_entry();

  m_large_columns.clear();
  m_total_allocated_for_read = 0;

  std::string base_err("Cannot start query: ");
  bool already_started = false;

  if(query_str.size() == 0) {
    already_started = true;
  }

  if(m_query_running && !already_started) {
    log_and_throw(base_err+"Already started.");
  }
  if(!m_inited) {
    log_and_throw(base_err+"Must initialize connection first.");
  }

  if(already_started) {
    m_query_running = true;
  }

  if(!already_started) {
    m_ret = SQLExecDirect(m_query_stmt, (SQLCHAR *)query_str.c_str(), SQL_NTS);
    handle_return(m_ret, "SQLExecDirect", m_query_stmt, SQL_HANDLE_STMT,
      "Failed to execute query");
  }

  m_ret = SQLNumResultCols(m_query_stmt, &m_num_result_cols);
  handle_return(m_ret, "SQLNumResultCols", m_query_stmt, SQL_HANDLE_STMT,
      "Unable to get number of columns of result!");

  logstream(LOG_INFO) << "Number of result columns: " << m_num_result_cols << std::endl;

  if(m_num_result_cols == 0) {
    return m_num_result_cols;
  } else {
    m_query_running = true;
  }

  SQLSMALLINT name_length;
  SQLSMALLINT data_type;
  SQLULEN column_size;
  SQLSMALLINT decimal_digits;
  SQLSMALLINT nullable;
  std::vector<SQLULEN> column_sizes(m_num_result_cols, 0);
  m_result_column_info.resize(m_num_result_cols);
  for(SQLUSMALLINT i = 1; i <= m_num_result_cols; ++i) {
    m_ret = SQLDescribeCol(m_query_stmt, i, m_name_buf, m_name_buf_len, &name_length, &data_type,
        &column_size, &decimal_digits, &nullable);
    handle_return(m_ret, "SQLDescribeCol", m_query_stmt, SQL_HANDLE_STMT,
        "Unable to get column description!");

    // Fill column info
    //TODO: Are all these casts safe??
    column_desc_t tmp_desc;
    tmp_desc.column_name = std::string((char *)m_name_buf);
    auto type_info = odbc_type_to_flex(data_type);
    tmp_desc.column_type = type_info.first;
    tmp_desc.column_c_type = type_info.second;
    tmp_desc.column_sql_type = data_type;

    // Enforces MY definition of "column size" (explained in the definition of the column_desc_t struct)
    if(tmp_desc.column_type == flex_type_enum::INTEGER) {
      tmp_desc.max_size_in_bytes = sizeof(flex_int);
    } else if(tmp_desc.column_type == flex_type_enum::FLOAT) {
      tmp_desc.max_size_in_bytes = sizeof(flex_float);
    } else if(tmp_desc.column_type == flex_type_enum::DATETIME) {
      tmp_desc.max_size_in_bytes = sizeof(TIMESTAMP_STRUCT);
    } else if(SQL_IS_INTERVAL(data_type)) {
      tmp_desc.max_size_in_bytes = sizeof(SQL_INTERVAL_STRUCT);
    } else {
      tmp_desc.max_size_in_bytes = size_t(column_size);

      // Room for null byte
      if(tmp_desc.column_c_type == SQL_C_CHAR)
        ++tmp_desc.max_size_in_bytes;
    }

    column_sizes[i-1] = tmp_desc.max_size_in_bytes;
    tmp_desc.num_decimal_digits = int(decimal_digits);
    tmp_desc.nullable = bool(nullable);

    m_result_column_info[i-1] = tmp_desc;
  }


  size_t row_in_bytes = 0;

  row_in_bytes = std::accumulate(column_sizes.begin(), column_sizes.end(), row_in_bytes);
  logstream(LOG_INFO) << "Row size in bytes: " << row_in_bytes << std::endl;
  if(row_in_bytes == 0) {
    this->finalize_query();
    log_and_throw("No data to retrieve; all columns have no data");
  }
  m_num_rows_to_fetch = this->calculate_batch_size(row_in_bytes);
  if(m_num_rows_to_fetch == 0) {
    std::stringstream err_msg;
    err_msg << "WARNING: The maximum size of one result row from this query ("
      << row_in_bytes << " bytes) will not fit in the allocated buffer ("
      << graphlab::ODBC_BUFFER_SIZE << " bytes).\nReading result rows in "
      "small chunks. If the data in any single row is bigger than your "
      "machine's memory, you will experience memory pressure.\nUse "
      "graphlab.set_runtime_config('GRAPHLAB_ODBC_BUFFER_SIZE', x) to adjust "
      "the size of the allocated buffer." << std::endl;
    logprogress_stream << err_msg.str();
    m_num_rows_to_fetch = 1;
  }

  m_ret = SQLSetStmtAttr(m_query_stmt, SQL_ATTR_ROWS_FETCHED_PTR, &m_num_rows_fetched, sizeof(m_num_rows_fetched));
  handle_return(m_ret, "SQLSetStmtAttr", m_query_stmt, SQL_HANDLE_STMT,
      "Failed to set place to get number of rows fetched.");

  m_ret = SQLSetStmtAttr(m_query_stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)m_num_rows_to_fetch, sizeof(m_num_rows_to_fetch));
  handle_return(m_ret, "SQLSetStmtAttr", m_query_stmt, SQL_HANDLE_STMT,
      "Failed to set block size for reading from DB.");

  ASSERT_TRUE(m_entry_buffer == NULL);
  ASSERT_TRUE(m_entry_buffer_size == NULL);
  m_entry_buffer = (SQLPOINTER *)malloc(m_num_result_cols * sizeof(SQLPOINTER));
  m_entry_buffer_size = (SQLLEN **)malloc(m_num_result_cols * sizeof(SQLLEN *));

  size_t vsi_size = sizeof(SQLLEN) * m_num_rows_to_fetch;
  for(SQLSMALLINT i = 0; i < m_num_result_cols; ++i) {
    // If a column is overly large, in order to fit into our buffer we don't
    // bind beforehand.  Instead we will continually call SQLGetData on it when
    // fetching. Because of the default behavior of SQLGetData, if the entire
    // row is too large for the buffer, we won't bind any columns.
    if((m_num_rows_to_fetch == 1) &&
       (row_in_bytes > graphlab::ODBC_BUFFER_SIZE)) {
      m_large_columns.insert(i);
      m_entry_buffer[i] = NULL;
      m_entry_buffer_size[i] = NULL;
      continue;
    }

    m_entry_buffer_size[i] = (SQLLEN *)malloc(vsi_size);
    auto bytes_to_alloc = (m_result_column_info[i].max_size_in_bytes *
        m_num_rows_to_fetch);
    m_entry_buffer[i] = (SQLPOINTER)malloc(bytes_to_alloc);
    m_total_allocated_for_read += bytes_to_alloc;
    memset(m_entry_buffer_size[i], 0, vsi_size);
    m_ret = SQLBindCol(m_query_stmt,
                     i+1,
                     m_result_column_info[i].column_c_type,
                     m_entry_buffer[i],
                     m_result_column_info[i].max_size_in_bytes,
                     m_entry_buffer_size[i]);
    handle_return(m_ret, "SQLBindCol", m_query_stmt, SQL_HANDLE_STMT,
        std::string("Failed to bind column ") + std::to_string(i) +
        std::string(" for reading!"));
  }

  // If we happened to find some huge columns, allocate a buffer to be used by
  // all of them
  //
  // TODO: We could do some heuristic here to allocate a small
  // amount here and realloc if we actually need it, since I think most people
  // will use this when the actual data size is way smaller than the max size
  // reported by the driver.  It will only be a performance improvement on OSs
  // that don't allow overcommitting of memory (i.e. Windows)
  if(m_large_columns.size() > 0) {
    m_large_column_buffer_size = graphlab::ODBC_BUFFER_SIZE-m_total_allocated_for_read;
    logstream(LOG_INFO) <<
      "Detected large row, allocating large column buffer: " <<
      m_large_column_buffer_size << " bytes" << std::endl;
    m_large_column_buffer = (SQLPOINTER)malloc(m_large_column_buffer_size);
  }


  return m_num_result_cols;
}

std::vector<std::vector<flexible_type>> odbc_connector::get_query_block() {
  if(!m_query_running) {
    log_and_throw("Query not started!");
  }

  if(cppipc::must_cancel()) {
    this->finalize_query();
    log_and_throw("Cancelled by user.");
  }

  std::vector<std::vector<flexible_type>> ret_block;

  m_ret = SQLFetch(m_query_stmt);

  if(m_ret == SQL_NO_DATA) {
    this->finalize_query();
    return ret_block;
  } else if(!SQL_SUCCEEDED(m_ret)) {
    handle_return(m_ret, "SQLFetch", m_query_stmt, SQL_HANDLE_STMT,
        "Error fetching the next set of results!");
  }

  ASSERT_TRUE(m_num_rows_fetched <= SQLLEN(m_num_rows_to_fetch));

  ret_block.resize(m_num_rows_fetched);
  for(SQLINTEGER j = 0; j < m_num_rows_fetched; ++j) {
    ret_block[j].resize(m_num_result_cols);
  }

  // This loop can be a bit confusing because we return results row-wise, but
  // here we fill them column-wise.
  for(SQLSMALLINT i = 0; i < m_num_result_cols; ++i) {
    //TODO: I think we'll throw if a type is not mapped to a gl type (undefined)
    auto c_type = m_result_column_info[i].column_c_type;

    // Special case for large values
    if(m_large_columns.find(i) != m_large_columns.end()) {
      ASSERT_TRUE(m_num_rows_fetched == 1);
      SQLLEN data_len;
      SQLRETURN rc;
      flex_string out_str = flex_string("");
      do {
        rc = SQLGetData(m_query_stmt, i+1, c_type, m_large_column_buffer,
            m_large_column_buffer_size, &data_len);
        handle_return(rc, "SQLGetData", m_query_stmt, SQL_HANDLE_STMT,
            "Error fetching large value!");

        SQLLEN num_bytes_to_append = 0;
        // These are the only possible C types this code will assign that are
        // variable in length
        if(SQL_IS_VARIABLE_LENGTH(m_result_column_info[i].column_sql_type) &&
            (data_len != SQL_NULL_DATA)) {
          if(data_len < 0) {
            this->finalize_query();
            log_and_throw("Invalid response from SQLGetData");
          } else if(data_len < m_large_column_buffer_size) {
            num_bytes_to_append = data_len;
          } else {
            num_bytes_to_append = m_large_column_buffer_size;
          }

          // If not null-terminated, we have to specify the number of bytes
          if(c_type != SQL_C_CHAR) {
            out_str.append((const char *)m_large_column_buffer, num_bytes_to_append);
          } else {
            out_str.append((const char *)m_large_column_buffer);
          }

        } else {
          flexible_type out;
          this->result_buffer_to_flexible_type(m_large_column_buffer, data_len, c_type, out);
          // There can only be one row read at a time if we are in this code path
          ret_block[0][i] = std::move(out);
        }
      } while((rc == SQL_SUCCESS_WITH_INFO) &&
          (std::strncmp((const char *)g_sql_state, (const char *)truncated_text_state, 5) == 0));

      // If we were appending the value, we need to add the finished value to
      // the result set out here
      if(out_str.size())
        ret_block[0][i] = std::move(out_str);
    } else {
      // Cast to char * to do pointer arithmetic
      // (sorry, but haven't found a way to avoid this yet)
      char *cur_data_pos = (char *)m_entry_buffer[i];

      for(SQLLEN j = 0; j < m_num_rows_fetched; ++j) {
        flexible_type out;
        result_buffer_to_flexible_type(cur_data_pos, m_entry_buffer_size[i][j], c_type, out);
        ret_block[j][i] = std::move(out);
        cur_data_pos += m_result_column_info[i].max_size_in_bytes;
      }
    }
  }

  return ret_block;
}

void odbc_connector::result_buffer_to_flexible_type(void *buffer_pos,
    SQLLEN elem_size, SQLSMALLINT c_type, flexible_type &out) {
  size_t elem_len = size_t(-1);
  bool is_null = false;
  if(elem_size == SQL_NULL_DATA) {
    out = FLEX_UNDEFINED;
    is_null = true;
  } else if(elem_size >= 0) {
    elem_len = elem_size;
  } else {
    out = FLEX_UNDEFINED;
    is_null = true;
  }

  if(!is_null) {
    this->sql_to_flexible_type(buffer_pos, elem_len, c_type, out);
  }
}

std::vector<std::string> odbc_connector::get_column_names() {
  std::vector<std::string> ret_names;
  if(!m_query_running) {
    log_and_throw("Query not started!");
  }

  ret_names.resize(m_num_result_cols);

  for(size_t i = 0; i < m_result_column_info.size(); ++i) {
    ret_names[i] = m_result_column_info[i].column_name;
  }

  return ret_names;
}

std::vector<flex_type_enum> odbc_connector::get_column_types() {
  std::vector<flex_type_enum> ret_types;

  if(!m_query_running) {
    log_and_throw("Query not started!");
  }

  ret_types.resize(m_num_result_cols);

  for(size_t i = 0; i < m_result_column_info.size(); ++i) {
    ret_types[i] = m_result_column_info[i].column_type;
  }

  return ret_types;
}

void odbc_connector::add_to_interval_dict(flex_dict &the_dict, std::string key, SQLUINTEGER value) {
  flex_int v = flex_int(value);

  the_dict.push_back(std::make_pair(key, v));
}

void odbc_connector::sql_to_flexible_type(void *buf, size_t len, SQLSMALLINT sql_c_type, flexible_type &out) {
  if(sql_c_type == SQL_C_TYPE_TIMESTAMP) {
    auto val = (TIMESTAMP_STRUCT *)buf;
    boost::posix_time::ptime the_time(
        boost::gregorian::date(val->year, val->month, val->day),
        boost::posix_time::time_duration(val->hour, val->minute, val->second));
    out = flex_date_time(flexible_type_impl::ptime_to_time_t(the_time), 0);
  } else if(sql_c_type == SQL_C_TYPE_DATE) {
    auto val = (DATE_STRUCT *)buf;
    boost::posix_time::ptime the_time(
        boost::gregorian::date(val->year, val->month, val->day));
    out = flex_date_time(flexible_type_impl::ptime_to_time_t(the_time), 0);
  } else if(sql_c_type == SQL_C_TYPE_TIME) {
    auto val = (TIME_STRUCT *)buf;
    boost::posix_time::ptime the_time(
        boost::gregorian::from_string("1970/1/1"),
        boost::posix_time::time_duration(val->hour, val->minute, val->second));
    out = flex_date_time(flexible_type_impl::ptime_to_time_t(the_time), 0);
  } else if(SQL_C_IS_INTERVAL(sql_c_type)) {
    // This comes in a rather silly structure.  I think the best way to present
    // this data to the user is in a python dictionary. This is because I don't
    // think there's a very compelling reason right now to add a "time
    // interval" type to flexible_type.  This type could possibly be translated
    // to a datetime.timedelta, but this doesn't cover years and months.
    // dateutil.relativedelta seems to fit the needs of this data type, but is
    // not python standard.
    //
    // I leave a tag for "_SQL_INTERVAL" which repeats for
    // every entry, but lets me know what this is when writing.  Unfortunately
    // I have no idea how people will actually want this data.  This is open
    // for debate!!
    flex_dict tmp_dict;
    auto val = (SQL_INTERVAL_STRUCT *)buf;
    tmp_dict.push_back(std::make_pair(std::string("_SQL_INTERVAL"),
                                      flex_int(1)
                                     ));
    tmp_dict.push_back(std::make_pair(std::string("_SQL_INTERVAL_SIGN"),
                                      (val->interval_sign == SQL_TRUE) ? 1 : 0));

    SQLUINTEGER cur_val;

    // Sanity
    if(val->interval_type < SQL_IS_YEAR ||
        val->interval_type > SQL_IS_MINUTE_TO_SECOND)
      log_and_throw("Attempted to read invalid interval type.");

    if(val->interval_type == SQL_IS_YEAR ||
       val->interval_type == SQL_IS_MONTH ||
       val->interval_type == SQL_IS_YEAR_TO_MONTH) {
      auto cur_struct = val->intval.year_month;
      cur_val = cur_struct.year;
      if(cur_val > 0)
        this->add_to_interval_dict(tmp_dict, std::string("year"), cur_val);

      cur_val = cur_struct.month;

      if(cur_val > 0 || tmp_dict.size() == 1)
        this->add_to_interval_dict(tmp_dict, std::string("month"), cur_val);
    } else {
      auto cur_struct = val->intval.day_second;
      cur_val = cur_struct.day;
      if(cur_val > 0)
        this->add_to_interval_dict(tmp_dict, std::string("day"), cur_val);
      cur_val = cur_struct.hour;
      if(cur_val > 0)
        this->add_to_interval_dict(tmp_dict, std::string("hour"), cur_val);
      cur_val = cur_struct.minute;
      if(cur_val > 0)
        this->add_to_interval_dict(tmp_dict, std::string("minute"), cur_val);
      cur_val = cur_struct.second;
      if(cur_val > 0 || tmp_dict.size() == 1)
        this->add_to_interval_dict(tmp_dict, std::string("second"), cur_val);
      cur_val = cur_struct.fraction;
      if(cur_val > 0)
        this->add_to_interval_dict(tmp_dict, std::string("fraction"), cur_val);
    }

    out = tmp_dict;
  } else if(sql_c_type == SQL_C_CHAR || sql_c_type == SQL_C_BINARY) {
    if(len == 0) {
      out = std::string("");
    } else {
      out = std::string((char *)buf, len);
    }
  } else if(sql_c_type == SQL_C_SBIGINT) {
    out = *(flex_int *)buf;
  } else if(sql_c_type == SQL_C_DOUBLE) {
    out = *(flex_float *)buf;
  } else {
    log_and_throw("Attempted to read unsupported SQL C type.");
  }
}

int odbc_connector::convert_str_to_time_code(std::string s) {
  if(s == std::string("year")) {
    return SQL_CODE_YEAR;
  } else if(s == std::string("month")) {
    return SQL_CODE_MONTH;
  } else if(s == std::string("day")) {
    return SQL_CODE_DAY;
  } else if(s == std::string("hour")) {
    return SQL_CODE_HOUR;
  } else if(s == std::string("minute")) {
    return SQL_CODE_MINUTE;
  } else if(s == std::string("second")) {
    return SQL_CODE_SECOND;
  } else if(s == std::string("fraction")) {
    return SQL_CODE_SECOND;
  }

  return SQL_CODE_YEAR-2;
}

void odbc_connector::add_to_interval_struct(SQL_INTERVAL_STRUCT &s, std::pair<flexible_type, flexible_type> entry) {
  if(entry.first == std::string("year")) {
    s.intval.year_month.year = (SQLUINTEGER)std::abs(entry.second.get<flex_int>());
  } else if(entry.first == std::string("month")) {
    s.intval.year_month.month = (SQLUINTEGER)std::abs(entry.second.get<flex_int>());
  } else if(entry.first == std::string("day")) {
    s.intval.day_second.day = (SQLUINTEGER)std::abs(entry.second.get<flex_int>());
  } else if(entry.first == std::string("hour")) {
    s.intval.day_second.hour = (SQLUINTEGER)std::abs(entry.second.get<flex_int>());
  } else if(entry.first == std::string("minute")) {
    s.intval.day_second.minute = (SQLUINTEGER)std::abs(entry.second.get<flex_int>());
  } else if(entry.first == std::string("second")) {
    s.intval.day_second.second = (SQLUINTEGER)std::abs(entry.second.get<flex_int>());
  } else if(entry.first == std::string("fraction")) {
    s.intval.day_second.fraction = (SQLUINTEGER)std::abs(entry.second.get<flex_int>());
  } else if(entry.first == std::string("_SQL_INTERVAL")) {
    //pass
  } else if(entry.first == std::string("_SQL_INTERVAL_SIGN")) {
    s.interval_sign = (entry.second.get<flex_int>() == SQL_TRUE) ? 1 : 0;
  } else {
    log_and_throw("Invalid interval entry!");
  }
}

SQLSMALLINT odbc_connector::identify_interval_type(flex_dict interval) {
  std::vector<int> time_codes;
  for(auto it = interval.begin(); it != interval.end(); ++it) {
    time_codes.push_back(this->convert_str_to_time_code(it->first));
  }

  auto max_val = *std::max_element(time_codes.begin(), time_codes.end());
  auto min_val = *std::min_element(time_codes.begin(), time_codes.end());
  auto interval_base = SQL_INTERVAL_YEAR - SQL_CODE_YEAR;
  if(min_val == max_val) {
    return interval_base + min_val;
  }

  if(min_val >= SQL_CODE_YEAR && min_val <= SQL_CODE_MONTH) {
    if(max_val == SQL_CODE_MONTH)
      return SQL_INTERVAL_YEAR_TO_MONTH;
  }

  if(min_val >= SQL_CODE_DAY && min_val <= SQL_CODE_SECOND) {
    if(min_val == SQL_CODE_DAY) {
      if(max_val == SQL_CODE_HOUR)
        return SQL_INTERVAL_DAY_TO_HOUR;
      else if(max_val == SQL_CODE_MINUTE)
        return SQL_INTERVAL_DAY_TO_MINUTE;
      else if(max_val == SQL_CODE_SECOND)
        return SQL_INTERVAL_DAY_TO_SECOND;
    } else if(min_val == SQL_CODE_HOUR) {
      if(max_val == SQL_CODE_MINUTE)
        return SQL_INTERVAL_HOUR_TO_MINUTE;
      else if(max_val == SQL_CODE_SECOND)
        return SQL_INTERVAL_HOUR_TO_SECOND;
    } else if(min_val == SQL_CODE_MINUTE) {
      if(max_val == SQL_CODE_SECOND)
        return SQL_INTERVAL_MINUTE_TO_SECOND;
    }
  }

  return SQL_INTERVAL_YEAR-1;
}

//TODO: Support type promotion (if no integer, promote to float, if no float,
// represent as string, etc.)
void odbc_connector::map_types_for_writing(sframe &sf, bool optimize_db_storage) {
  log_func_entry();

  if(!m_inited) {
    log_and_throw("Cannot map types for writing before init!");
  }

  if(!m_db_type_info.size()) {
    log_and_throw("No DB-specific type information found. Cannot write!");
  }

  if(m_column_write_info.size()) {
    log_and_throw("Type mapping appears to have already taken place!");
  }

  if(optimize_db_storage) {
    log_and_throw("DB storage optimization mode not implemented yet!");
  }

  auto sf_cols = sf.num_columns();
  m_column_write_info.resize(sf_cols);

  for(size_t i = 0; i < sf_cols; ++i) {
    auto cur_type = sf.column_type(i);
    column_desc_t cur_column_desc;
    std::string cur_param_field;

    // Set data for the attributes we need to check on each column
    auto limits = this->get_column_limits(sf.select_column(i), optimize_db_storage);
    // Mark unsigned
    cur_column_desc.unsigned_attribute = SQL_FALSE;
    if(cur_type == flex_type_enum::INTEGER) {
      if(limits.first >= 0)
        cur_column_desc.unsigned_attribute = SQL_TRUE;
    }

    // Set column size
    if(cur_type == flex_type_enum::INTEGER) {
      // For integers, this means the number of digits you need to use to
      // display the text of the biggest integer (not including '-' symbol)
      auto big_str = std::to_string(
          std::max(std::abs(limits.first.get<flex_int>()),
            std::abs(limits.second.get<flex_int>())));
      cur_column_desc.column_size = (flex_int)big_str.size();
    } else if(cur_type == flex_type_enum::FLOAT) {
      // Number of bits in mantissa
      cur_column_desc.column_size = (flex_int)std::numeric_limits<flex_float>::digits;
    } else {
      cur_column_desc.column_size = (flex_int)limits.second;
    }

    // Set fixed_precision
    if(cur_type == flex_type_enum::FLOAT) {
      cur_column_desc.fixed_precision = SQL_FALSE;
    }

    // Set decimal digits
    bool invalid_type = false;
    if(cur_type == flex_type_enum::DATETIME ||
        cur_type == flex_type_enum::DICT) {
      //TODO: This should be something
      cur_column_desc.num_decimal_digits = 0;
    } else {
      cur_column_desc.num_decimal_digits = 0;
    }

    // Set max bytes (oh, and double check the validity of types)
    if(cur_type == flex_type_enum::STRING) {
      cur_column_desc.max_size_in_bytes = limits.second+1;
    } else if(cur_type == flex_type_enum::IMAGE) {
      cur_column_desc.max_size_in_bytes = limits.second;

      // This case is roughly the same as string, but I'm putting support for
      // this off.  I don't think there will be much demand.  There's support
      // for it in a lot of places...just not everywhere
      log_and_throw("Writing images to the DB is not supported at this time.");
    } else if(cur_type == flex_type_enum::INTEGER) {
      cur_column_desc.max_size_in_bytes = sizeof(flex_int);
    } else if(cur_type == flex_type_enum::FLOAT) {
      cur_column_desc.max_size_in_bytes = sizeof(flex_float);
    } else if(cur_type == flex_type_enum::DATETIME) {
      cur_column_desc.max_size_in_bytes = sizeof(TIMESTAMP_STRUCT);
    } else if(cur_type == flex_type_enum::DICT) {
      // Check if this is an interval column.  If there is at least one
      // interval, we'll call this an interval column.
      
      // The thought here is that if we successfully find an SQL interval, 
      // it will happen early because the user is trying to convert intervals, 
      // so there will be one early.
      sarray_reader<flexible_type> qk_checker;
      qk_checker.init(*sf.select_column(i), size_t(1));
      bool interval_column = false;

      // This is not a parallel check because of the short circuiting.  I
      // expect short circuits to be the common case for the user doing the
      // right thing.  The overall common case is probably the user erroneously
      // trying to submit dictionaries to the DB. In this case it will take a
      // little longer, but at least won't hit the database, which would be
      // even longer.  Sorry wrong user, you don't get a parallel check to give
      // you an error.
      size_t cnt = 0;
      for(auto it = qk_checker.begin(0); it != qk_checker.end(0); ++it, ++cnt) {
        if((*it).get_type() != flex_type_enum::DICT) {
          continue;
        }
        auto the_dict = (*it).get<flex_dict>();
        auto find_ret = std::find(the_dict.begin(), the_dict.end(),
            std::pair<flexible_type, flexible_type>(std::string("_SQL_INTERVAL"), 1));
        if(find_ret != the_dict.end()) {
          interval_column = true;

          // Figure out what type of interval this is
          auto tmp = this->identify_interval_type(the_dict);
          if(tmp < SQL_INTERVAL_YEAR || tmp > SQL_INTERVAL_MINUTE_TO_SECOND) {
            log_and_throw("Could not identify interval type!");
          }
          cur_column_desc.column_sql_type = tmp;
          break;
        }
      }

      if(!interval_column)
        invalid_type = true;
      cur_column_desc.max_size_in_bytes = sizeof(SQL_INTERVAL_STRUCT);
    } else {
      invalid_type = true;
    }

    if(invalid_type) {
      log_and_throw("ODBC does not support inserting list-like objects.\n  "
          "Please remove all list/array/dict columns (unless the dict "
          "represents a time interval) or convert them to a supported type.");
    }

    auto f2s_ret = m_flex2sql_types.find(cur_type);
    if(f2s_ret == m_flex2sql_types.end() || !f2s_ret->second.size()) {
      log_and_throw(std::string("Could not match type ") +
          flex_type_enum_to_name(cur_type) + std::string(" to a SQL type."));
    }

    // For each possible SQL equivalent to this flexible type, retrieve all
    // DB-specific types that map to the valid SQL equivalent types along with
    // the types' metadata (as returned by SQLGetTypeInfo).  Resulting vector
    // db_type_info_indexes is in priority order.
    auto allowable_sql_types = f2s_ret->second;
    std::vector<size_t> db_type_info_indexes;
    for(auto sql_type_it = allowable_sql_types.begin();
        sql_type_it != allowable_sql_types.end(); ++sql_type_it) {

      auto find_ret = m_db_type_info_by_sql_type.find(*sql_type_it);

      // If we don't find any DB-specific type for this SQL type, then we just
      // move on.
      if(find_ret == m_db_type_info_by_sql_type.end()) {
        continue;
      }
      std::vector<size_t> index_vec = find_ret->second;
      db_type_info_indexes.insert(db_type_info_indexes.end(),
          index_vec.begin(), index_vec.end());
    }

    // Okay, here's the actual loop for finding a DB-specific type
    size_t cur_match_idx = size_t(-1);
    bool match_instead;
    flexible_type tmp_flex;
    for(auto it = db_type_info_indexes.begin(); it != db_type_info_indexes.end(); ++it) {
      match_instead = false;
      ASSERT_TRUE(*it < m_db_type_info.size());
      ASSERT_TRUE(*it != cur_match_idx);
      auto cur_type_info = m_db_type_info[*it];

      // We absolutely need these fields for this to matter
      if(cur_type_info[type_info_keys::TYPE_NAME].get_type() != flex_type_enum::STRING ||
          cur_type_info[type_info_keys::DATA_TYPE].get_type() != flex_type_enum::INTEGER) {
        log_and_throw("Driver gives malformed type information.  Cannot write.");
      }

      // Check for autoincrement columns (which means you can't insert)
      tmp_flex = cur_type_info[type_info_keys::AUTO_UNIQUE_VALUE];
      if(tmp_flex.get_type() == flex_type_enum::INTEGER &&
          (SQLSMALLINT)tmp_flex.get<flex_int>() == SQL_TRUE) {
        continue;
      }

      // I would check for nullable, but I don't think drivers handle that too well

      // Check sign
      tmp_flex = cur_type_info[type_info_keys::UNSIGNED_ATTRIBUTE];
      if(cur_type == flex_type_enum::INTEGER && tmp_flex.get_type() == flex_type_enum::INTEGER) {
        if(cur_column_desc.unsigned_attribute !=
            (SQLSMALLINT)tmp_flex.get<flex_int>()) {
          // This is not a match!
          continue;
        }
      }

      // Check fixed/approximate
      // This should be checked for integer, but some drivers don't use this correctly
      tmp_flex = cur_type_info[type_info_keys::FIXED_PREC_SCALE];
      if(cur_type == flex_type_enum::FLOAT && tmp_flex.get_type() == flex_type_enum::INTEGER) {
        auto db_type_fixed = tmp_flex.get<flex_int>();
        if(cur_column_desc.fixed_precision != db_type_fixed) {
          continue;
        }
      }

      // Check precision 
      tmp_flex = cur_type_info[type_info_keys::COLUMN_SIZE];
      flex_int cur_column_size = tmp_flex.get<flex_int>();

      // If this driver doesn't tell us how big ANYTHING is, then this is a no go.
      if(tmp_flex.get_type() != flex_type_enum::INTEGER)
        log_and_throw("Driver does not give type size information.  Cannot write.");
      if(cur_type == flex_type_enum::INTEGER) {
        if(cur_column_desc.column_size > cur_column_size) {
          continue;
        }
        //TODO: Further type matching to do if this check succeeds, but only in
        //optimize_db_storage mode.  Can't do checks like below, because column
        //size is only part of the story for integers.
      } else if(cur_type == flex_type_enum::FLOAT) {
        //TODO: I can't find a reliable way to check this right now.  Too many
        //drivers give too many different answers.
      } else if(cur_type == flex_type_enum::STRING ||
          cur_type == flex_type_enum::IMAGE) {
        if(cur_column_desc.column_size <= cur_column_size) {
          // First, prefer varying character types, then prefer smallest this
          // column allows.  If string, only match binary types if a character
          // type hasn't been matched
          if(cur_match_idx != size_t(-1)) {
            auto prev_match_data_type = m_db_type_info[cur_match_idx][type_info_keys::DATA_TYPE];
            auto cur_sql_type = cur_type_info[type_info_keys::DATA_TYPE];

            // If we have matched a string type, don't go and match this to a binary type!
            if(cur_type == flex_type_enum::STRING &&
                cur_sql_type <= SQL_BINARY &&
                cur_sql_type >= SQL_LONGVARBINARY)
              continue;

            if((!SQL_IS_VARYING(prev_match_data_type)) &&
                (SQL_IS_VARYING(cur_sql_type))) {
              match_instead = true;
            } else if((SQL_IS_VARYING(prev_match_data_type)) == (SQL_IS_VARYING(cur_sql_type))) {
              if(cur_column_size < m_db_type_info[cur_match_idx][type_info_keys::COLUMN_SIZE]) {
                match_instead = true;
              }
            }
          }
        } else {
          continue;
        }
      } else if(cur_type == flex_type_enum::DATETIME) {
        // Don't check, but prefer smaller column size. We assume if this meets
        // the requirements to be called a timestamp, it will fit our needs.  
        if(cur_match_idx != size_t(-1)) {
          auto prev_match = m_db_type_info[cur_match_idx];
          if(cur_column_size < prev_match[type_info_keys::COLUMN_SIZE]) {
            match_instead = true;
          }
        }
      } else if(cur_type == flex_type_enum::DICT) {
        //TODO: For now, I'm just blindly accepting intervals, no matter if
        //they lose precision.
      }

      // Check if this is the correct interval type
      if(cur_type == flex_type_enum::DICT) {
        // column_sql_type shouldn't be set beforehand to any other type
        tmp_flex = cur_type_info[type_info_keys::DATA_TYPE];
        if(tmp_flex.get_type() != flex_type_enum::INTEGER ||
            cur_column_desc.column_sql_type != tmp_flex.get<flex_int>())
          continue;
      }

      auto create_params = cur_type_info[type_info_keys::CREATE_PARAMS];
      std::string tmp_param_field;
      if(create_params.get_type() == flex_type_enum::STRING) {
        auto p = create_params.get<flex_string>();
        if(p.size() > 0) {
          // Right now, I only support handling length parameters
          boost::algorithm::to_lower<std::string>(p);
          if(!boost::algorithm::contains(p, "length") || boost::algorithm::contains(p, ",")) {
            continue;
          } else {
            tmp_param_field = std::string("(") +
              std::to_string(cur_type_info[type_info_keys::COLUMN_SIZE].get<flex_int>()) +
              std::string(")");
          }
        }
      }

      // If we got here, this means at least we found no reason NOT to accept
      // this. If match_instead is marked, then we want to replace a match if
      // one was already made. Otherwise, an earlier match gets priority over 
      // a later one (ODBC type output is in priority order).
      if(match_instead || cur_match_idx == size_t(-1)) {
        cur_match_idx = *it;
        cur_param_field = tmp_param_field;
      }
    }

    if(cur_match_idx == size_t(-1)) {
      log_and_throw(std::string("Cannot find matching database type for data in column ") + std::string(std::to_string(i)));
    }

    // Fill type information in the column_desc_t
    auto final_match = m_db_type_info[cur_match_idx];

    cur_column_desc.db_specific_type =
      final_match[type_info_keys::TYPE_NAME].get<flex_string>() + cur_param_field;
    cur_column_desc.column_sql_type =
      final_match[type_info_keys::DATA_TYPE].get<flex_int>();
    auto other_types = odbc_type_to_flex(cur_column_desc.column_sql_type);
    cur_column_desc.column_type = other_types.first;
    cur_column_desc.column_c_type = other_types.second;

    m_column_write_info[i] = cur_column_desc;
  }
}

/// Only defined when the column type is integer for float
static std::pair<flexible_type, flexible_type>
get_array_limits(std::shared_ptr<sarray<flexible_type>> column) {
  flexible_type min_val, max_val;
  auto cur_type = column->get_type();
  if(cur_type == flex_type_enum::INTEGER) {
    min_val = std::numeric_limits<flex_int>::max();
    max_val = std::numeric_limits<flex_int>::min();
  } else if(cur_type == flex_type_enum::FLOAT) {
    min_val = std::numeric_limits<flex_float>::max();
    max_val = std::numeric_limits<flex_float>::min();
  } else {
    ASSERT_MSG(false, "Array limits only defined for numeric column types.");
  }

  auto reader = column->get_reader(1);
  auto iter = reader->begin(0);
  auto enditer = reader->end(0);
  while (iter != enditer) {
    const auto& val = (*iter);
    if (val.get_type() != flex_type_enum::UNDEFINED) {
      if (val < min_val) min_val = val;
      if (val > max_val) max_val = val;
    }
    ++iter;
  }
  return {min_val, max_val};
}

std::pair<flexible_type, flexible_type> odbc_connector::get_column_limits(
    std::shared_ptr<sarray<flexible_type>> column, bool optimize_db_storage) {
  bool is_max = true;
  flexible_type fdefault = 0;

  // Simple lambda function to calculate the max size of types that are varying
  // size.  We need this to allocate enough buffer space for a row ahead of
  // time, as ODBC wants it.
  auto max_datalen_fn = [is_max](flexible_type f, flexible_type &cur_max_len) {
    size_t f_size = 0;
    switch(f.get_type()) {
      case flex_type_enum::STRING:
        f_size = f.get<flex_string>().size();
        break;
      case flex_type_enum::VECTOR:
      case flex_type_enum::LIST:
      case flex_type_enum::DICT:
      case flex_type_enum::INTEGER:
      case flex_type_enum::FLOAT:
      case flex_type_enum::DATETIME:
        return false;
      case flex_type_enum::UNDEFINED:
        return true;
      //TODO: This probably doesn't work for image types.  I probably have to
      //worry about saving and loading and the size after that and such. You
      //probably shouldn't be loading images into an RDBMS anyways.
      case flex_type_enum::IMAGE:
        f_size = f.get<flex_image>().m_image_data_size;
        break;
    }
    if(is_max) {
      if(f_size > cur_max_len) {
        cur_max_len = f_size;
      }
    } else {
      if(f_size < cur_max_len) {
        cur_max_len = f_size;
      }
    }

    return true;
  };

  switch(column->get_type()) {
    case flex_type_enum::INTEGER:
      if(optimize_db_storage) {
        return get_array_limits(column);
      } else {
        flexible_type fmin, fmax;
        fmax = std::numeric_limits<flex_int>::max();
        fmin = std::numeric_limits<flex_int>::min();
        return std::make_pair(fmin, fmax);
      }
      break;
    case flex_type_enum::FLOAT:
      if(optimize_db_storage) {
        return get_array_limits(column);
      } else {
        flexible_type fmin, fmax;
        fmax = std::numeric_limits<flex_float>::max();
        fmin = std::numeric_limits<flex_float>::min();
        return std::make_pair(fmin, fmax);
      }
      break;
    case flex_type_enum::IMAGE:
    case flex_type_enum::STRING:
    {
      // Since we don't have a limit internally on strings, we have to do a
      // pass over the data to see how large the strings are...we don't have an
      // option to skip it.
      flexible_type init = flex_int(0);
      auto results = graphlab::reduce(*column, max_datalen_fn, init);
      auto max_val = *std::max_element(results.begin(), results.end());
      return std::make_pair(max_val, max_val);
    }
    //TODO: I think I could measure the fraction here
    case flex_type_enum::DICT:
    case flex_type_enum::DATETIME:
      break;
    default:
      log_and_throw("Invalid type for writing to DB!");
      break;
  }

  return std::make_pair(fdefault, fdefault);
}

void odbc_connector::map_types_for_writing() {
  log_func_entry();

  if(!m_inited) {
    log_and_throw("Cannot map types for writing before init!");
  }

  // Map flexible types to SQL types. This is a list of all acceptable SQL
  // types for each flexible_type we allow, in priority order.  This is used by
  // the sframe-specific map_types_for_writing call.
  m_flex2sql_types.insert(std::make_pair(flex_type_enum::STRING,
                                       std::vector<SQLSMALLINT> { 

                                                          SQL_VARCHAR,
                                                          SQL_LONGVARCHAR,
                                                          SQL_CHAR,
                                                          SQL_VARBINARY,
                                                          SQL_LONGVARBINARY,
                                                          SQL_BINARY,

                                                        }));
  m_flex2sql_types.insert(std::make_pair(flex_type_enum::FLOAT,
                                       std::vector<SQLSMALLINT> {

                                                          SQL_DOUBLE,
                                                          SQL_REAL,

                                                        }));
  m_flex2sql_types.insert(std::make_pair(flex_type_enum::INTEGER,
                                       std::vector<SQLSMALLINT> { 

                                                          SQL_TINYINT,
                                                          SQL_SMALLINT,
                                                          SQL_INTEGER,
                                                          SQL_BIGINT,

                                                        }));
  m_flex2sql_types.insert(std::make_pair(flex_type_enum::DATETIME,
                                       std::vector<SQLSMALLINT> {

                                       SQL_TYPE_TIMESTAMP,

                                       }));
  m_flex2sql_types.insert(std::make_pair(flex_type_enum::IMAGE,
                                       std::vector<SQLSMALLINT> {

                                       SQL_VARBINARY,
                                       SQL_LONGVARBINARY,
                                       SQL_BINARY,

                                       }));
  m_flex2sql_types.insert(std::make_pair(flex_type_enum::DICT,
                                       std::vector<SQLSMALLINT> {
                                        
                                       SQL_INTERVAL_YEAR,
                                       SQL_INTERVAL_MONTH,
                                       SQL_INTERVAL_DAY,
                                       SQL_INTERVAL_HOUR,
                                       SQL_INTERVAL_MINUTE,
                                       SQL_INTERVAL_SECOND,
                                       SQL_INTERVAL_YEAR_TO_MONTH,
                                       SQL_INTERVAL_DAY_TO_HOUR,
                                       SQL_INTERVAL_DAY_TO_MINUTE,
                                       SQL_INTERVAL_DAY_TO_SECOND,
                                       SQL_INTERVAL_HOUR_TO_MINUTE,
                                       SQL_INTERVAL_HOUR_TO_SECOND,
                                       SQL_INTERVAL_MINUTE_TO_SECOND,

                                                          }));

  m_types_mapped = true;
}

std::string odbc_connector::make_create_table_string(sframe &sf, const std::string &table_name) {
  auto names = sf.column_names();

  if(m_column_write_info.size() != names.size()) {
    log_and_throw("Cannot create table before m_column_write_info initialized!");
  }

  std::string cur_quote_char((char *)m_identifier_quote_char);

  std::stringstream ss_out;
  ss_out << "CREATE TABLE " << table_name << " (";

  for(size_t i = 0; i < names.size(); ++i) {
    ss_out << cur_quote_char << names[i] << cur_quote_char << " ";

    // Get the correct spelling of our best guess of corresponding type
    ss_out << m_column_write_info[i].db_specific_type;
    if(i != (names.size() - 1))
      ss_out << ",";
  }

  ss_out << ")";

  return ss_out.str();
}

bool odbc_connector::create_table(sframe &sf, const std::string &table_name) {
  log_func_entry();

  if(!m_types_mapped) {
    log_and_throw("Cannot create table: DB types not mapped.");
  }

  m_ret = SQLTables(m_query_stmt, NULL, 0, NULL, 0, NULL, 0, (SQLCHAR *)"TABLE", SQL_NTS);
  handle_return(m_ret, "SQLTables", m_query_stmt, SQL_HANDLE_STMT,
      "Unable to get list of tables!");

  // Sent a request to get all table names, now get the result and store
  sframe tables;
  bool tables_exist = get_query_result_as_sframe(tables, "");
  ASSERT_MSG(tables_exist, "No table list available!");

  // If this table exists in the database, attempt to append
  auto table_names = tables.select_column("TABLE_NAME");
  bool table_found = false;
  std::string current_query;

  logstream(LOG_INFO) << tables.num_rows() << std::endl;
  // This data ain't indexed for us, so we search linearly
  //TODO: Handle internal ODBC result sets uniformly.
  auto rdr = table_names->get_reader();
  for(size_t i = 0; i < rdr->num_segments(); ++i) {
    for(auto iter = rdr->begin(i); iter != rdr->end(i); ++iter) {
      auto tmp = *iter;
      std::stringstream to_test;
      to_test << m_identifier_quote_char << (tmp).mutable_get<flex_string>() << m_identifier_quote_char;
      if(table_name == to_test.str()) {
        table_found = true;
        break;
      }
    }
  }

  // If table does not exist, create it
  if(!table_found) {
    current_query = this->make_create_table_string(sf, table_name);
    logstream(LOG_INFO) << "SQL submit: " << current_query << std::endl;
    ASSERT_TRUE(this->start_query(current_query) == 0);
  }

  return !table_found;
}

void odbc_connector::insert_data(sframe &sf, const std::string &table_name, bool append_if_exists, bool auto_create_table) {
  log_func_entry();

  std::stringstream ss;
  ss << m_identifier_quote_char << table_name << m_identifier_quote_char;
  std::string table_name_to_be_used = ss.str();

  auto input_columns = sf.num_columns();
  if(input_columns < 1) {
    log_and_throw("Must have at least one column to write!");
  }
  try {
    this->insert_data_impl(sf, table_name_to_be_used, append_if_exists, auto_create_table);
  } catch(...) {
    // I know catch(...) can be bad practice, but this stuff ABSOLUTELY has to
    // happen to avoid memory leaks and to keep the DB from being in a weird
    // state.
    SQLEndTran(SQL_HANDLE_DBC, m_dbc, SQL_ROLLBACK);

    this->finalize_insert(input_columns);

    if(m_table_created) {
      ASSERT_TRUE(this->start_query(std::string("DROP TABLE ") + table_name_to_be_used) == 0);
    }

    throw;
  }

  this->finalize_insert(input_columns);
  SQLEndTran(SQL_HANDLE_DBC, m_dbc, SQL_COMMIT);
}

void odbc_connector::finalize_insert(size_t num_columns) {
    if(m_row_bound_params) {
      for(size_t i = 0; i < num_columns; ++i) {
        if(m_row_bound_params[i])
          free(m_row_bound_params[i]);
      }

      free(m_row_bound_params);
      m_row_bound_params = NULL;
    }

    if(m_value_size_indicator) {
      for(size_t i = 0; i < num_columns; ++i) {
        if(m_value_size_indicator[i])
          free(m_value_size_indicator[i]);
      }
      free(m_value_size_indicator);
      m_value_size_indicator = NULL;
    }

    m_ret = SQLSetConnectAttr(m_dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, SQL_IS_UINTEGER);

    if(m_insert_stmt)
      SQLFreeHandle(SQL_HANDLE_STMT, m_insert_stmt);

    m_column_write_info.clear();
}

//TODO: What happens if the table exists but doesn't have the correct schema?
void odbc_connector::insert_data_impl(sframe &sf, const std::string &table_name, bool append_if_exists, bool auto_create_table) {
  log_func_entry();
  m_table_created = false;

  size_t num_rows_to_submit = 0;

  // Allocate a separate statement for each insert
  SQLHSTMT m_insert_stmt;
  m_ret = SQLAllocHandle(SQL_HANDLE_STMT, m_dbc, &m_insert_stmt);
  handle_return(m_ret, "SQLAllocHandle", m_dbc, SQL_HANDLE_DBC, "Failed to allocate statement object");

  // This sets the m_column_write_info vector, needed for the rest of this stuff
  this->map_types_for_writing(sf);


  if(auto_create_table) {
    m_table_created = this->create_table(sf, table_name);
  }

  //TODO: Check existing columns to see if types match up.  Try to adjust
  //accordingly. Right now we just blindly insert a set row of values.
  if(!m_table_created) {
    
  }

  if(!m_table_created && !append_if_exists) {
    log_and_throw("Table already exists!");
  }

  // Don't autocommit every insert statement
  m_ret = SQLSetConnectAttr(m_dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
  handle_return(m_ret, "SQLSetConnectAttr", m_dbc, SQL_HANDLE_DBC, "Failed to set autocommit off");

  size_t input_columns = sf.num_columns();
  size_t input_rows = sf.num_rows();
  auto input_types = sf.column_types();

  // Put together the query we'll use for all of our inserting
  auto insert_str = std::string("INSERT INTO ") + table_name + std::string(" VALUES (");
  for(size_t i = 0; i < input_columns; ++i) {
    insert_str += std::string(1, '?');
    if((i+1) != input_columns) {
      insert_str += std::string(1, ',');
    }
  }
  insert_str += std::string(1, ')');

  // In some cases SQLPrepare needs to be called before SQLBindParameter, so
  // it's up here to be safe
  m_ret = SQLPrepare(m_insert_stmt, (SQLCHAR *)insert_str.c_str(), SQL_NTS);
  handle_return(m_ret, "SQLPrepare", m_insert_stmt, SQL_HANDLE_STMT, "Failed to prepare insert statement");


  size_t row_bytes = 0;
  for(size_t i = 0; i < input_columns; ++i) {
    row_bytes += m_column_write_info[i].max_size_in_bytes;
  }

  if(row_bytes == 0) {
    log_and_throw("No data to write!");
  }

  logstream(LOG_INFO) << "Row size to write: " << row_bytes << " bytes" << std::endl;
  num_rows_to_submit = this->calculate_batch_size(row_bytes);
  if(num_rows_to_submit == 0) {
    std::stringstream err_msg;
    err_msg << "WARNING: The maximum size of one row from this SFrame ("
      << row_bytes << " bytes) will not fit in the allocated buffer ("
      << graphlab::ODBC_BUFFER_SIZE << " bytes).\n Use graphlab.set_runtime_"
      "config('GRAPHLAB_ODBC_BUFFER_SIZE', x) to adjust it." << std::endl;
    log_and_throw(err_msg.str());
  }

  // I think the last parameter is ignored in this instance
  m_ret = SQLSetStmtAttr(m_insert_stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)num_rows_to_submit, 0);
  handle_return(m_ret, "SQLSetStmtAttr", m_insert_stmt, SQL_HANDLE_STMT, "Failed to set attribute for bulk insertion");

  // Allocate memory for the pointers to the buffer for each column that will be inserted
  ASSERT_TRUE(m_row_bound_params == NULL);
  m_row_bound_params = (SQLPOINTER *)malloc(input_columns * sizeof(SQLPOINTER));

  // Each column needs an array of size of num_rows_to_submit indicating the
  // size in bytes of that specific element. It is only necessary for character
  // or binary strings and columns that may include NULL data (so most data
  // found in SFrames).  This allocates the memory for pointers.  The second
  // level is allocated within the loop.
  ASSERT_TRUE(m_value_size_indicator == NULL);
  m_value_size_indicator = (SQLLEN **)malloc(sizeof(SQLLEN *) * input_columns);

  // Bind each parameter (the '?' character in the insert statement) to a chunk
  // of memory.  This simply allocates the memory and passes it to
  // SQLBindParameter, along with a bunch of other stuff we need to know.  Once
  // we fill this memory, we'll call SQLExecute, which will send all of that
  // data to the ODBC driver.
  for(size_t i = 0; i < input_columns; ++i) {
    m_row_bound_params[i] =(SQLPOINTER)malloc(
        m_column_write_info[i].max_size_in_bytes * num_rows_to_submit);

    // Make sure this represents an empty string if the memory doesn't get filled
    if(input_types[i] == flex_type_enum::STRING)
      memset(m_row_bound_params[i], 0, 1);

    // Second level of m_value_size_indicator allocated HERE
    m_value_size_indicator[i] = (SQLLEN *)malloc(sizeof(SQLLEN) * num_rows_to_submit);
    memset(m_value_size_indicator[i], 0, num_rows_to_submit);

    m_ret = SQLBindParameter(m_insert_stmt,
                           i+1,
                           SQL_PARAM_INPUT,
                           m_column_write_info[i].column_c_type,
                           m_column_write_info[i].column_sql_type,
                           m_column_write_info[i].column_size,
                           m_column_write_info[i].num_decimal_digits,
                           m_row_bound_params[i],
                           (SQLLEN)m_column_write_info[i].max_size_in_bytes,
                           &m_value_size_indicator[i][0]);
  }

  size_t cur_row_in_buffer = 0;
  size_t cur_row = 0;
  bool submit_now = false;

  // Get all information needed to iterate through given SFrame
  sframe_reader sf_rdr;
  sf_rdr.init(sf, 1);
  auto tbl_iter = sf_rdr.begin(0);

  for(; tbl_iter != sf_rdr.end(0); ++tbl_iter, ++cur_row) {
    size_t elem_num = 0;

    // This just iterates over a single row
    for(auto row_iter = tbl_iter->begin(); row_iter != tbl_iter->end(); ++row_iter, ++elem_num) {
      auto cur_type = row_iter->get_type();

      // We need to multiplex on each possible type and copy the memory to the
      // buffer we bound above in that type's special way.
      if(cur_type == flex_type_enum::UNDEFINED) {
        // ODBC's null indicator
        m_value_size_indicator[elem_num][cur_row_in_buffer] = SQL_NULL_DATA;
      } else if(cur_type == flex_type_enum::STRING) {
        auto strified = row_iter->to<std::string>();
        auto cur_elem_size = strified.size()+1;

        if(cur_elem_size > m_column_write_info[elem_num].max_size_in_bytes) { 
          std::stringstream err_msg;
          err_msg << "Row " << cur_row << " \"" << strified << "\"" <<
            " is too big for buffer size of column " << elem_num << " '" <<
            m_column_write_info[elem_num].column_name << "' (" <<
            cur_elem_size << " > " <<
            m_column_write_info[elem_num].max_size_in_bytes << ").";
          log_and_throw(err_msg.str());
        }

        memcpy((char *)m_row_bound_params[elem_num] +
            (cur_row_in_buffer*m_column_write_info[elem_num].max_size_in_bytes),
            strified.c_str(), cur_elem_size);

        // Tells ODBC that this is a null-terminated string, so no need to
        // calculate size
        m_value_size_indicator[elem_num][cur_row_in_buffer] = SQL_NTS;
      } else if(cur_type == flex_type_enum::FLOAT) {
        flex_float tmp = flex_float(*row_iter);
        memcpy((double *)m_row_bound_params[elem_num] + cur_row_in_buffer, &tmp, sizeof(flex_float));
        m_value_size_indicator[elem_num][cur_row_in_buffer] = sizeof(flex_float);
      } else if(cur_type == flex_type_enum::INTEGER) {
        flex_int tmp = flex_int(*row_iter);
        memcpy((int64_t *)m_row_bound_params[elem_num] + cur_row_in_buffer, &tmp, sizeof(flex_int));
        m_value_size_indicator[elem_num][cur_row_in_buffer] = sizeof(flex_int);
      } else if(cur_type == flex_type_enum::DATETIME) {
        flex_date_time dt = row_iter->get<flex_date_time>();

        boost::posix_time::ptime ptime_val = flexible_type_impl::ptime_from_time_t(dt.shifted_posix_timestamp(), dt.microsecond());
        tm _tm = boost::posix_time::to_tm(ptime_val); 

        TIMESTAMP_STRUCT to_sql_struct;
        to_sql_struct.year = (SQLSMALLINT)(_tm.tm_year + 1900);
        to_sql_struct.month = (SQLUSMALLINT)_tm.tm_mon + 1;
        to_sql_struct.day = (SQLUSMALLINT)_tm.tm_mday;
        to_sql_struct.hour = (SQLUSMALLINT)_tm.tm_hour;
        to_sql_struct.minute = (SQLUSMALLINT)_tm.tm_min;
        to_sql_struct.second = (SQLUSMALLINT)_tm.tm_sec;
        to_sql_struct.fraction = (SQLUINTEGER)0;

        memcpy((TIMESTAMP_STRUCT *)m_row_bound_params[elem_num]+cur_row_in_buffer,
            &to_sql_struct, sizeof(TIMESTAMP_STRUCT));
        m_value_size_indicator[elem_num][cur_row_in_buffer] = sizeof(TIMESTAMP_STRUCT);
      } else if(cur_type == flex_type_enum::DICT) {
        flex_dict fdict = row_iter->get<flex_dict>();
        auto interval_base = SQL_INTERVAL_YEAR - SQL_CODE_YEAR;
        SQL_INTERVAL_STRUCT to_sql_struct;
        to_sql_struct.interval_type = (SQLINTERVAL)(m_column_write_info[elem_num].column_sql_type - interval_base);
        for(auto iter = fdict.begin(); iter != fdict.end(); ++iter) {
          this->add_to_interval_struct(to_sql_struct, *iter);
        }
        memcpy((SQL_INTERVAL_STRUCT *)m_row_bound_params[elem_num]+cur_row_in_buffer, &to_sql_struct, sizeof(SQL_INTERVAL_STRUCT));
        m_value_size_indicator[elem_num][cur_row_in_buffer] = sizeof(SQL_INTERVAL_STRUCT);
      }
    }

    ++cur_row_in_buffer;

    if(cur_row == (input_rows-1)) {
      submit_now = true;
      logstream(LOG_INFO) << "Last row at " << cur_row_in_buffer << " in buffer." << std::endl;
    }

    // Rollback transcation if failure
    if((cur_row_in_buffer == num_rows_to_submit) || submit_now) {
      if(submit_now) {
        m_ret = SQLSetStmtAttr(m_insert_stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)cur_row_in_buffer, 0);
        handle_return(m_ret, "SQLSetStmtAttr", m_insert_stmt, SQL_HANDLE_STMT, "Failed to set attribute for bulk insertion");
      }

      if(cppipc::must_cancel()) {
        throw("Cancelled by user.");
      }
      m_ret = SQLExecute(m_insert_stmt);
      handle_return(m_ret, "SQLExecute", m_insert_stmt, SQL_HANDLE_STMT, "Failure to execute insert statement!");

      cur_row_in_buffer = 0;

      logprogress_stream_ontick(15) << std::setprecision(1) << std::fixed <<
        cur_row+1 << " rows have been inserted (" <<
        (double(cur_row+1) / double(input_rows))*100.0 << "%)" << std::endl;
    }
  }

  logprogress_stream << cur_row << " rows have been inserted (100.0%)" << std::endl;
}

bool odbc_connector::get_query_result_as_sframe(sframe &sf, std::string query_str) {
  // Start query and get result information
  auto num_result_cols = this->start_query(query_str);

  if(!num_result_cols) {
    return false;
  }

  auto names = this->get_column_names();
  auto types = this->get_column_types();

  sf.open_for_write(names, types);
  auto sf_iter = sf.get_output_iterator(0);

  auto result_row_set = this->get_query_block();
  while(result_row_set.size() > 0) {
    for(auto it = result_row_set.begin(); it != result_row_set.end(); ++it) {
      (*sf_iter) = std::move(*it);
    }
    result_row_set = this->get_query_block();
  }

  sf.close();

  return true;
}

}

/*
 * TODO: Document state machine and make sure all the coding follows it...make
 * actual states and an interface to it
 *
 * TODO: Possibly get size of query (if known) and plan segment layout(/parallelize?)
 * TODO: Get more than one row/make an iterator
 * TODO: Fully lazy SFrame/handle more than one concurrent transaction
 * TODO: Handle intricacies of time and intervals better
 * TODO: Think of possible user knobs:
 * TODO: Handle special characters and unicode
 */
