class db_config:
    mongo_db_uri = "mongodb://localhost:27017"
    mongo_db_name = "WeaveAI"
    mdb_cache_content_collection_name = "content_cache"
    vdb_cache_content_collection_name = "content_cache"
    mdb_cache_summary_collection_name = "summary_cache"
    
    course_config = "course_config" #Can be referenced using Student ID
    course_quiz = "course_quiz" # Module Code: quiz_<module>_<submodule>
    course_questionnaire = "course_questionnaire"
    course_content = "course_content" # Module Code: content_module_submodule
    course_content_raw = "course_content_cache"
    course_summaries = "course_summaries"

    redis_host="localhost"
    redis_port=6379
    redis_db=0
    redis_password=None #str
    redis_decode_responses=True