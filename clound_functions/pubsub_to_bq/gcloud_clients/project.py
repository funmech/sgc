import google.auth


class Info:
    """Default GCP project information"""

    def __init__(self):
        """Get current project_id from current GCP credentials"""
        _, self.project_id = google.auth.default()
        super().__init__()
