# lws_workflow/exceptions.py

class WorkflowHold(Exception):
    """Stop processing intentionally (not a failure). Can carry created item codes."""
    def __init__(self, message: str, created_items: list[str] | None = None):
        super().__init__(message)
        self.created_items = created_items or []


class WorkflowApiError(Exception):
    """
    Raised when an XLink API call fails and we want the Admin email to include
    structured API details even when LOG_LEVEL=INFO.
    """
    def __init__(
        self,
        message: str,
        *,
        api_entity: str | None = None,
        api_status: int | None = None,
        api_error_message: str | None = None,
        api_messages: list | None = None,
        raw_response_text: str | None = None,
    ):
        super().__init__(message)
        self.api_entity = api_entity
        self.api_status = api_status
        self.api_error_message = api_error_message
        self.api_messages = api_messages or []
        self.raw_response_text = raw_response_text
