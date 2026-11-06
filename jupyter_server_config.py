print('pipeline_db_content_manager LOADED')
c.ServerApp.contents_manager_class = (
    "pipeline_db_content_manager.manager.DBContentManager"
)
c.LanguageManager.locale = "vi_VN"
