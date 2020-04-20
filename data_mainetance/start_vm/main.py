from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()

def start_vm(request):
    def start_engine(compute_instance='instance-3', project='rental-organizer',zone='us-central1-a'):
        service = discovery.build('compute', 'v1', credentials=credentials)
        request = service.instances().start(
            project=project, zone=zone, instance=compute_instance)
        request.execute()

    start_engine()
