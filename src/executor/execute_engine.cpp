#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include "common/config.h"

#define CREATE_NEW_FILE


ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.**/
#ifndef CREATE_NEW_FILE
  LOG(INFO)<<"Do not use new file"<<endl;
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
#endif
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(db_name.c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement finished
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  auto catalog_manager = dbs_[current_db_]->catalog_mgr_;
  string table_name = ast->child_->val_;
  vector<Column *>columns;
  auto def = ast->child_->next_;
  if(def==nullptr){
    LOG(WARNING)<<"A table must have at least 1 column"<<endl;
    return DB_FAILED;
  }

  //get columns
  int column_id=0;//TODO:column begins with 0 here.
  def = def->child_;
  while(def!=nullptr){
    if(def->val_!=nullptr&&string(def->val_)=="primary keys"){
      break;
    }
    auto column_name = def->child_->val_;
    auto column_type_raw = def->child_->next_->val_;
    TypeId column_type;
    uint32_t length=0;
    bool nullable=true;
    bool unique=false;

    if(def->val_!=nullptr){
      if(string(def->val_)=="unique"){
        unique=true;
        nullable=false;//TODO:maybe can be true?
      }
    }

    if(string(column_type_raw)=="int")column_type = kTypeInt;
    if(string(column_type_raw)=="float")column_type = kTypeFloat;
    if(string(column_type_raw)=="char"){
      column_type = kTypeChar;
      length = atoi(def->child_->next_->child_->val_);
    }

    Column* column;
    if(column_type==kTypeChar)column = new Column(column_name,column_type,length,column_id,nullable,unique);
    else column = new Column(column_name,column_type,column_id,nullable,unique);
    columns.emplace_back(column);

    column_id++;
    def=def->next_;
  }

  //处理主键
  Column* primary_column;
  if(def!=nullptr&&def->val_!=nullptr&&string(def->val_)=="primary keys"){
    auto key_name = string(def->child_->val_);
    for(auto column : columns){
      if(column->GetName()==key_name){
        primary_column=column;
        column->SetNullable(false);
        column->SetUnique(true);
        break;
      }
    }
  }

  auto schema = new Schema(columns,true); 
  //TODO:txn here.
  TableInfo* table_info;
  auto exe_info = catalog_manager->CreateTable(table_name,schema,nullptr,table_info);
  if(exe_info==DB_SUCCESS){
    cout<<"Successfully create table"<<endl;
#ifndef CREATE_INDEX_ON_UNIQUE
    //TODO:not implemented yet
//    catalog_manager->CreateIndex(table_name,)
#endif
  }
  return exe_info;
}

/**
 * TODO: Student Implement finished
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  auto catalog_manager = dbs_[current_db_]->catalog_mgr_;
  auto exe_info = catalog_manager->DropTable(ast->child_->val_);
  if(exe_info==DB_SUCCESS){
    cout<<"Successfully drop table"<<endl;
  }
  return exe_info;
}

/**
 * TODO: Student Implement finished
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  auto catalog_manager = dbs_[current_db_]->catalog_mgr_;
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;

  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Tables"
       << " | " << std::left << setfill(' ') << setw(max_width) << "Indexes" 
       << " |" << endl;

  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  vector<TableInfo *>tables;
  catalog_manager->GetTables(tables);
  for (const auto &table : tables) {
    vector<IndexInfo*>indexes;
    catalog_manager->GetTableIndexes(table->GetTableName(),indexes);
    for(const auto &index : indexes)
    {
      cout << "| " << std::left << setfill(' ') << setw(max_width) << table->GetTableName() << " | " << 
                      std::left << setfill(' ') << setw(max_width) << index->GetIndexName() << " |"<< endl;
    }
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement finished
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  auto catalog_manager = dbs_[current_db_]->catalog_mgr_;
  auto index_name = ast->child_->val_;
  auto table_name = ast->child_->next_->val_;
  auto index_keys = ast->child_->next_->next_;
  string index_type = "bptree";
  if(index_keys->next_!=nullptr)index_type = index_keys->next_->child_->val_;
  if(string(index_type)!="bptree"){
    cout<<"unsupported index type of "<<index_type<<"(now only support bptree)"<<endl;
    return DB_FAILED;
  }
  vector<std::string>index_keys_array;
  auto index_value=index_keys->child_;
  while(index_value!=nullptr){
    index_keys_array.emplace_back(string(index_value->val_));
    index_value=index_value->next_;
  }
  IndexInfo* index_info;
  //TODO: Transaction txn
#ifdef ENABLE_EXECUTE_DEBUG
  cout<<"create on "<<table_name<<"Index Columns:\n========"<<endl;
  for(auto i:index_keys_array){
    cout<<i<<endl;
  }
  cout<<"=========\n";
#endif
  auto exe_info = catalog_manager->CreateIndex(table_name,index_name,index_keys_array,nullptr,index_info,index_type);
  if(exe_info==DB_SUCCESS){
    cout<<"Successfully create index"<<endl;
  }
  return exe_info;
}

/**
 * TODO: Student Implement finished
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  auto catalog_manager = dbs_[current_db_]->catalog_mgr_;
  string index_name = string(ast->child_->val_);
  uint32_t drop_tot=0;
  auto exe_info = catalog_manager->DropAllIndexes(index_name,drop_tot);
    if(exe_info==DB_SUCCESS){
    cout<<"Successfully drop "<<drop_tot<<" index"<<endl;
  }
  return exe_info;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement finished
 * Warning: please
 */


extern "C" {
int yyparse(void);
extern FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
#include <parser/syntax_tree_printer.h>
#include <utils/tree_file_mgr.h>
void InputFileCommand(char *input, const int len,std::ifstream& file) {
  memset(input, 0, len);
//  printf("minisql > ");
  //  fflush(stdout);
  int i = 0;
  char ch;
  while ((ch = file.get()) != ';') {
    if(file.eof())return;
    input[i++] = ch;
  }
  input[i] = ch;  // ;
  file.get();      // remove enter
}
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name = ast->child_->val_;
  const int buf_size = 1024;
  char cmd[buf_size];
  std::ifstream file(file_name,std::ifstream::in);
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_file_");
  uint32_t syntax_tree_id=0;
  if(file.is_open()){
    while(!file.eof())
    {
      InputFileCommand(cmd, buf_size, file);
      YY_BUFFER_STATE bp = yy_scan_string(cmd);
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
        exit(1);
      }
      yy_switch_to_buffer(bp);

      // init parser module
      MinisqlParserInit();

      // parse
      yyparse();

      // parse result handle
      if (MinisqlParserGetError()) {
        // error
        printf("%s\n", MinisqlParserGetErrorMessage());
      } else {
        // Comment them out if you don't need to debug the syntax tree
        #ifdef ENABLE_SYNTAX_DEBUG
        printf("[INFO] Sql syntax parse ok!\n");
        SyntaxTreePrinter printer(MinisqlGetParserRootNode());
        printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
        #endif
      }

      auto result = Execute(MinisqlGetParserRootNode());

      // clean memory after parse
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();

      // quit condition
      ExecuteInformation(result);
      if (result == DB_QUIT) {
        exit(0);
      }
    }
    cout<<"Successfully execute all the command in the file!"<<endl;
    return DB_SUCCESS;
  }
  else return DB_FAILED;
}

/**
 * TODO: Student Implement finished
*/
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_QUIT;
}
