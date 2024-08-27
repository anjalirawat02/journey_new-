from django.db.models import Count, F
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db.models import Count, Q, F
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from .models import Journey, JourneyEvents, AddToJobs, CandidateJourney, CandidateDetails, CandidateStatuses, CampaignTriggers,JobDetails,SegmentCategories,BotDetails, Campaigns,CampaignEvent,CampaignChannel,JourneyEventHiringManager,Segments,StepType,SubmitPanel,JourneyEventHiringManager,SubmitClient,Client,JobCandidateHistories,Assessment,InterviewType
from .serializers import JourneySerializer, JourneyEventsSerializer, JobDashboardSerializer, CandidateSerializer,AddToJobsSerializer,JobDetailsSerializer,CandidateJourneySerializer,CampaignEventsSerializer,CampaignTriggersSerializer
from rest_framework import generics
from django.http import JsonResponse
from django.views import View
import logging
from rest_framework.views import APIView
from django.http import QueryDict
from django.db.models import Count
from .models import Journey, JourneyEvents, AddToJobs, CandidateJourney, CandidateDetails, CandidateStatuses, CampaignTriggers,JobDetails,SegmentCategories,BotDetails, Campaigns,CampaignEvent,CampaignChannel,JourneyEventHiringManager,Segments,StepType,SubmitPanel,JourneyEventHiringManager,SubmitClient,Client,JobCandidateHistories,Assessment,InterviewType
from .serializers import JourneySerializer, JourneyEventsSerializer, JobDashboardSerializer, CandidateSerializer,AddToJobsSerializer,JobDetailsSerializer,CandidateJourneySerializer,CampaignEventsSerializer,CampaignTriggersSerializer
logger = logging.getLogger('journey')

class JobDashboardView(View):
    def get(self, request, job_id):
        response_data = {}
        
        serializer = JobDashboardSerializer(data={'job_id': job_id, 'status': request.GET.get('status')})
        if not serializer.is_valid():
            logger.warning(f"Validation error: {serializer.errors}")
            return JsonResponse({'error': serializer.errors}, status=400)
        
        job_id = serializer.validated_data['job_id']
        status = serializer.validated_data['status']

        try:
            logger.info(f"Fetching details for job ID: {job_id}")
            journey = JobDetails.objects.get(id=job_id)
            journey_id = journey.journey_id

            logger.info(f"Fetching candidate statuses for status: {status}")
            status_objs = CandidateStatuses.objects.filter(root_name=status)
            status_id_to_display_name = {obj.id: obj.display_name for obj in status_objs}
            status_ids = list(status_id_to_display_name.keys())

            logger.info(f"Fetching journey events for journey ID: {journey_id}")
            
            # Fetch all journey events for the journey
            journey_events = JourneyEvents.objects.filter(journey_id=journey_id, is_deleted=False)

            for journey_event in journey_events:
                journey_event_id = journey_event.id
                interview_type = journey_event.interview_type

                logger.info(f"Fetching status counts for journey event ID: {journey_event_id}")

                # Check if there are any CampaignTriggers for this journey event
                campaign_triggers = CampaignTriggers.objects.filter(
                    journey_event_id=journey_event_id,
                    journey_id=journey_id,
                    job_id=job_id,
                    status_id__in=status_ids
                ).exclude(id=F('parent_id'))  

                event_name = f"{interview_type}"
                response_data[event_name] = {}

                if campaign_triggers.exists():
                    status_counts = campaign_triggers.values('status_id').annotate(count=Count('status_id'))

                    for status_count in status_counts:
                        status_id = status_count['status_id']
                        count = status_count['count']
                        display_name = status_id_to_display_name[status_id]
                        
                        if self.candidate_journey_has_completed(campaign_triggers, status_id):
                            count = 0

                        response_data[event_name][display_name] = {
                            'count': count,
                            'status_id': status_id,
                            'journey_id': journey_id,
                            'journey_event_id': journey_event_id,
                            'job_id': job_id
                        }
                else:
                    response_data[event_name] = {'message': 'Campaign has not started for this journey event'}

            logger.info(f"Successfully fetched data for job ID: {job_id}")
            return JsonResponse(response_data)

        except JobDetails.DoesNotExist:
            logger.warning(f"Job with ID {job_id} not found")
            return JsonResponse({'error': f'Job with ID "{job_id}" not found'}, status=400)
        except JourneyEvents.DoesNotExist:
            logger.warning(f"No journey events found for job ID {job_id}")
            return JsonResponse({'error': f'No journey events found for job ID "{job_id}"'}, status=200)
        except CandidateStatuses.DoesNotExist:
            logger.error("No status IDs found")
            return JsonResponse({'error': 'No status IDs found'}, status=200)
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return JsonResponse({'error': 'An unexpected error occurred'}, status=500)

    def candidate_journey_has_completed(self, campaign_triggers, status_id):
        # Helper function to check if any associated CandidateJourney has campaign_status as 'completed' for a given status_id
        triggers = campaign_triggers.filter(status_id=status_id)
        for trigger in triggers:
            candidate_journey = CandidateJourney.objects.get(id=trigger.candidate_journey_id)
            if candidate_journey.campaign_status == 'completed':
                return True
        return False

  
@method_decorator(csrf_exempt, name='dispatch')
class CandidateView(View):
    def post(self, request):
        try:
            data = request.POST 
            
            # If the data is not in POST, convert QueryDict to dict
            if isinstance(data, QueryDict):
                data = data.dict()
                
            serializer = CandidateSerializer(data=data)
            if not serializer.is_valid():
                raise ValidationError(serializer.errors)

            validated_data = serializer.validated_data
            status_id = validated_data.get('status_id')
            journey_id = validated_data.get('journey_id')
            journey_event_id = validated_data.get('journey_event_id')
            job_id = validated_data.get('job_id')
            
            # Set default values for start_index and end_index
            start_index = validated_data.get('start_index', 0)
            end_index = validated_data.get('end_index', 10)
            
            # Logging and further processing
            logger.info(f"Fetching candidate IDs for status_id: {status_id}, journey_id: {journey_id}, journey_event_id: {journey_event_id}, job_id: {job_id}")
            
            # Fetching candidate IDs and corresponding add_to_job_id matching criteria
            candidate_triggers = CampaignTriggers.objects.filter(
                status_id=status_id,
                journey_id=journey_id,
                journey_event_id=journey_event_id,
                job_id=job_id
            ).values('candidate_id', 'add_to_job_id')

            # Extract candidate_ids and create a dictionary for add_to_job_id
            candidate_ids = [ct['candidate_id'] for ct in candidate_triggers]
            candidate_add_to_job_ids = {ct['candidate_id']: ct['add_to_job_id'] for ct in candidate_triggers}

            # Total count of candidates 
            total_candidates = len(candidate_ids)
            
            # Fetching limited candidate details
            candidates_queryset = CandidateDetails.objects.filter(id__in=candidate_ids).values()[start_index:end_index]
            
            # Prepare response data
            response_data = []
            for candidate in candidates_queryset:
                candidate_id = candidate['id']
                candidate['totalcandidate'] = total_candidates
                candidate['status_id'] = status_id
                candidate['job_id'] = job_id
                candidate['display_name'] = 'Sourced'
                candidate['add_to_job_id'] = candidate_add_to_job_ids.get(candidate_id)
                response_data.append(candidate)

            logger.info(f"Successfully fetched candidate data")
            
            return JsonResponse(response_data, safe=False)
            
        except ValidationError as ve:
            logger.error(f"Validation error: {ve}")
            return JsonResponse({'error': ve.detail}, status=400)
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return JsonResponse({'error': 'An unexpected error occurred'}, status=500)

class RejectedJobDashboardView(APIView):
    def get(self, request):
        response_data = {}
          # Initialize a single total_count for all events
        job_id=request.query_params.get('job_id')
        serializer = JobDashboardSerializer(data={'job_id': job_id, 'status': 'Rejected'})
        if not serializer.is_valid():
            logger.warning(f"Validation error: {serializer.errors}")
            return JsonResponse({'error': serializer.errors}, status=400)
        
        job_id = serializer.validated_data['job_id']
        status = serializer.validated_data['status']

        try:
            logger.info(f"Fetching details for job ID: {job_id}")
            journey = JobDetails.objects.get(id=job_id)
            journey_id = journey.journey_id

            logger.info(f"Fetching candidate statuses for status: {status}")
            status_objs = CandidateStatuses.objects.filter(root_name=status)
            status_id_to_display_name = {obj.id: obj.display_name for obj in status_objs}
            status_ids = list(status_id_to_display_name.keys())

            logger.info(f"Fetching journey events for journey ID: {journey_id}")
            
            # Fetch all journey events for the journey
            journey_events = JourneyEvents.objects.filter(journey_id=journey_id, is_deleted=False)

            for journey_event in journey_events:
                journey_event_id = journey_event.id
                interview_type = journey_event.interview_type

                logger.info(f"Fetching status counts for journey event ID: {journey_event_id}")

                # Check if there are any CampaignTriggers for this journey event
                campaign_triggers = CampaignTriggers.objects.filter(
                    journey_event_id=journey_event_id,
                    journey_id=journey_id,
                    job_id=job_id,
                    status_id__in=status_ids
                ).exclude(id=F('parent_id'))  

                event_name = f"{interview_type}"
                response_data[event_name] = {}

                if campaign_triggers.exists():
                    status_counts = campaign_triggers.values('status_id').annotate(count=Count('status_id'))

                    for status_count in status_counts:
                        status_id = status_count['status_id']
                        count = status_count['count']
                        display_name = status_id_to_display_name[status_id]
                        
                        total_count += count  # Add to the single total_count

                        response_data[event_name][display_name] = {
                            'count': count,
                            'status_id': status_id,
                            'journey_id': journey_id,
                            'journey_event_id': journey_event_id,
                            'job_id': job_id
                        }
                else:
                    response_data[event_name] = {'message': 'Campaign has not started for this journey event'}

            # Add total_count to the response
            response_data['total_count'] = total_count

            logger.info(f"Successfully fetched data for job ID: {job_id}")
            return JsonResponse(response_data)

        except JobDetails.DoesNotExist:
            logger.warning(f"Job with ID {job_id} not found")
            return JsonResponse({'error': f'Job with ID "{job_id}" not found'}, status=400)
        except JourneyEvents.DoesNotExist:
            logger.warning(f"No journey events found for job ID {job_id}")
            return JsonResponse({'error': f'No journey events found for job ID "{job_id}"'}, status=200)
        except CandidateStatuses.DoesNotExist:
            logger.error("No status IDs found")
            return JsonResponse({'error': 'No status IDs found'}, status=200)
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return JsonResponse({'error': 'An unexpected error occurred'}, status=500)
        

# @method_decorator(csrf_exempt, name='dispatch')
# class CombinedDashboardView(APIView):
#     def get(self, request):
#         try:
#             # Fetch and validate input parameters
#             is_count = int(request.GET.get('is_count', 0))
#             start_index = int(request.GET.get('start_index', 0))
#             end_index = int(request.GET.get('end_index', 10))
#             status = request.GET.get('status')
#             job_id = int(request.GET.get('job_id'))

#             response_data = {}
#             status_id_to_display_name = {}

#             # Fetch journey details based on job_id
#             logger.info(f"Fetching journey details for job ID: {job_id}")
#             journey_entries = AddToJobs.objects.filter(job_id=job_id)

#             if not journey_entries.exists():
#                 logger.warning(f"Job with ID {job_id} not found")
#                 return JsonResponse({'error': f'Job with ID "{job_id}" not found'}, status=400)

#             # Handle Leakage status separately before the main loop
#             if status == 'Leakage':
#                 journey_data = {}
#                 for journey_entry in journey_entries:
#                     total_count = 0
#                     journey_id = journey_entry.journey_id
#                     journey_name = Journey.objects.filter(id=journey_id).first().name
#                     journey_events = JourneyEvents.objects.filter(journey_id=journey_id, is_deleted=False)

#                     for journey_event in journey_events:
#                         journey_event_id = journey_event.id
#                         cancelled_candidates_counts = self.get_cancelled_candidates_with_counts(journey_id, journey_event_id)

#                         if cancelled_candidates_counts:
#                             status_ids = [entry['status_id'] for entry in cancelled_candidates_counts]
#                             status_objs = CandidateStatuses.objects.filter(id__in=status_ids)
#                             status_id_to_display_name.update({obj.id: obj.display_name for obj in status_objs})

#                             event_name = journey_event.interview_type
#                             journey_data[event_name] = {}
#                             for status_count in cancelled_candidates_counts:
#                                 status_id = status_count['status_id']
#                                 count = status_count['count']
#                                 display_name = status_id_to_display_name.get(status_id, 'Unknown')

#                                 journey_data[event_name][display_name] = {
#                                     'count': count,
#                                 }
#                                 total_count += count

#                                 if is_count == 0:
#                                     candidate_ids = self.get_cancelled_candidates(journey_id, journey_event_id).filter(status_id=status_id).values_list('candidate_id', flat=True)
#                                     candidates_queryset = CandidateDetails.objects.filter(id__in=candidate_ids).values()[start_index:end_index]
#                                     journey_data[event_name][display_name]['candidates'] = list(candidates_queryset)
#                         else:
#                             event_name = journey_event.interview_type
#                             journey_data[event_name] = {'count': 0}

#                     journey_data['total_count'] = total_count
#                     response_data[journey_name] = journey_data

#                 # Skip the rest of the processing as Leakage is handled separately
#                 logger.info(f"Successfully fetched data for job ID: {job_id} with Leakage status")
#                 return JsonResponse(response_data)

#             # For other statuses, proceed as before
#             for journey_entry in journey_entries:
#                 total_count = 0
#                 journey_id = journey_entry.journey_id
#                 journey_name = Journey.objects.filter(id=journey_id).first().name
#                 journey_data = {}

#                 # Fetch relevant status and journey event data
#                 logger.info(f"Fetching candidate statuses for status: {status}")
#                 status_objs = CandidateStatuses.objects.filter(root_name=status)
#                 status_id_to_display_name.update({obj.id: obj.display_name for obj in status_objs})
#                 status_ids = list(status_id_to_display_name.keys())

#                 logger.info(f"Fetching journey events for journey ID: {journey_id}")
#                 journey_events = JourneyEvents.objects.filter(journey_id=journey_id, is_deleted=False)

#                 for journey_event in journey_events:
#                     journey_event_id = journey_event.id
#                     interview_type = journey_event.interview_type
#                     event_name = f"{interview_type}"
#                     journey_data[event_name] = {}

#                     campaign_triggers = CampaignTriggers.objects.filter(
#                         journey_event_id=journey_event_id,
#                         journey_id=journey_id,
#                         job_id=job_id,
#                         status_id__in=status_ids,
#                     ).exclude(id=F('parent_id'))

#                     if campaign_triggers.exists():
#                         # Get all status_ids from campaign_triggers
#                         status_ids_in_triggers = campaign_triggers.values_list('status_id', flat=True).distinct()
#                         status_candidate_counts = {status_id: 0 for status_id in status_ids_in_triggers}

#                         # Fetch and filter triggers based on is_next_action
#                         triggers_with_count = campaign_triggers.values('status_id', 'candidate_id', 'is_next_action')

#                         for trigger in triggers_with_count:
#                             status_id = trigger['status_id']
#                             candidate_id = trigger['candidate_id']
#                             is_next_action = trigger['is_next_action']
                            
#                             if is_next_action == 0:
#                                 if status_id in status_candidate_counts:
#                                     status_candidate_counts[status_id] += 1

#                         for status_id in status_ids_in_triggers:
#                             count = status_candidate_counts.get(status_id, 0)
#                             display_name = status_id_to_display_name.get(status_id, 'Unknown')

#                             journey_data[event_name][display_name] = {
#                                 'count': count,
#                             }
#                             total_count += count

#                             if is_count == 0:
#                                 candidate_ids = campaign_triggers.filter(is_next_action=0, status_id=status_id).values_list('candidate_id', flat=True)
#                                 candidates_queryset = CandidateDetails.objects.filter(id__in=candidate_ids).values()[start_index:end_index]
#                                 journey_data[event_name][display_name]['candidates'] = list(candidates_queryset)
#                     else:
#                         # Handle the case where campaign_triggers does not exist
#                         candidate_journeys = CandidateJourney.objects.filter(
#                             journey_id=journey_id,
#                             journey_event_id=journey_event_id,
#                             job_id=job_id
#                         ).values('candidate_id', 'campaign_status')

#                         if candidate_journeys.exists():
#                             status_messages = {}
#                             for cj in candidate_journeys:
#                                 campaign_status = cj['campaign_status']
#                                 if campaign_status == 0:
#                                     status_messages[cj['candidate_id']] = 'Campaign not started for this event'
#                                 else:
#                                     status_messages[cj['candidate_id']] = f'Campaign is {campaign_status} for this event'
#                             journey_data[event_name] = {'message': status_messages}
#                         else:
#                             journey_data[event_name] = {'message': 'Campaign has not started for this event'}

#                 journey_data['total_count'] = total_count
#                 response_data[journey_name] = journey_data

#             logger.info(f"Successfully fetched data for job ID: {job_id}")
#             return JsonResponse(response_data)

#         except Exception as e:
#             logger.error(f"An unexpected error occurred: {e}")
#             return JsonResponse({'error': 'An unexpected error occurred'}, status=500)

#     def candidate_journey_has_completed(self, campaign_triggers, status_id):
#         triggers = campaign_triggers.filter(status_id=status_id)
#         for trigger in triggers:
#             candidate_journey = CandidateJourney.objects.get(id=trigger.candidate_journey_id)
#             if candidate_journey.campaign_status == 'completed':
#                 return True
#         return False

#     def get_cancelled_candidates(self, journey_id, journey_event_id):
#         # Helper function to return only cancelled candidates for Leakage status
#         return CandidateJourney.objects.filter(
#             journey_id=journey_id,
#             journey_event_id=journey_event_id,
#             campaign_status='cancelled'
#         ).values('candidate_id', 'status_id')

#     def get_cancelled_candidates_with_counts(self, journey_id, journey_event_id):
#         # Helper function to return only cancelled candidates for Leakage status with counts
#         return CandidateJourney.objects.filter(
#             journey_id=journey_id,
#             journey_event_id=journey_event_id,
#             campaign_status='cancelled'
#         ).values('status_id').annotate(count=Count('candidate_id', distinct=True))

# from django.db import connection

# class CombinedDashboardView(APIView):
#     def get(self, request):
#         try:
#             is_count = int(request.GET.get('is_count', 0))
#             start_index = int(request.GET.get('start_index', 0))
#             end_index = int(request.GET.get('end_index', 10))
#             status = request.GET.get('status')
#             job_id = int(request.GET.get('job_id'))

#             with connection.cursor() as cursor:
#                 cursor.callproc('GetDashboardData', [job_id, status, is_count, start_index, end_index])
#                 result = cursor.fetchall()

#             # Convert the result to a JSON response
#             response_data = []
#             for row in result:
#                 data = {
#                     'journey_name': row[0],
#                     'event_name': row[1],
#                     'display_name': row[2]
#                 }
#                 if is_count == 1:
#                     data['count'] = row[3]
#                 else:
#                     data['candidates'] = row[4]
#                 response_data.append(data)

#             return JsonResponse(response_data, safe=False)

#         except Exception as e:
#             return JsonResponse({'error': str(e)}, status=500)


from django.http import JsonResponse
from django.db import connection
from rest_framework.views import APIView
import json

class CombinedDashboardView(APIView):
    def get(self, request):
        try:
            is_count = int(request.GET.get('is_count', 0))
            start_index = int(request.GET.get('start_index', 0))
            end_index = int(request.GET.get('end_index', 10))
            status = request.GET.get('status')
            subprocess = request.GET.get('subprocess')
            job_id = int(request.GET.get('job_id'))

            with connection.cursor() as cursor:
                cursor.callproc('GetDashboardData', [job_id, status, subprocess, is_count, start_index, end_index])
                result = cursor.fetchall()

            response_data = []
            if is_count == 1:
                # When is_count is 1, we want to group data by journey, event, and status
                for row in result:
                    journey_name = row[0]
                    event_name = row[1]
                    display_name = row[2]
                    count = row[3]
                    response_data.append({
                        'journey_name': journey_name,
                        'event_name': event_name,
                        'display_name': display_name,
                        'count': count
                    })
            else:
                # When is_count is 0, we want to flatten the candidates list
                candidates = []
                for row in result:
                    candidate_json = row[4]
                    if candidate_json:
                        candidates.extend(json.loads(candidate_json))
                
                response_data = {'candidates': candidates}

            return JsonResponse(response_data, safe=False)

        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
