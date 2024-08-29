rom django.db.models import Count, F
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
from django.core.cache import cache
from django.http import JsonResponse
from django.db import connection
import json
from rest_framework.views import APIView
from django.http import QueryDict
from django.db.models import Count
logger = logging.getLogger('journey')





class CombinedDashboardView(APIView):
    def get(self, request):
        try:
            # Retrieve query parameters
            is_count = int(request.GET.get('is_count', 0))
            start_index = int(request.GET.get('start_index', 0))
            end_index = int(request.GET.get('end_index', 10))
            status = request.GET.get('status')
            subprocess = request.GET.get('subprocess')
            job_id = int(request.GET.get('job_id'))

            # Generate a unique cache key
            cache_key = f"dashboard_{job_id}_{status}_{subprocess}_{is_count}_{start_index}_{end_index}"

            # Check if the result is already cached
            cached_data = cache.get(cache_key)
            if cached_data:
                return JsonResponse(cached_data, safe=False)

            # Execute the stored procedure if cache miss
            with connection.cursor() as cursor:
                cursor.callproc('GetDashboardData', [job_id, status, subprocess, is_count, start_index, end_index])
                result = cursor.fetchall()

            # Initialize response_data
            response_data = {
                "total_count": 0,
                "journeys": []
            }

            if is_count == 1:
                # Group data by journey, event, and status
                journey_dict = {}
                total_count = 0

                for row in result:
                    journey_name = row[0]
                    event_name = row[1]
                    display_name = row[2]
                    count = row[3]
                    journey_id = row[5]
                    journey_event_id = row[6]
                    status_id = row[7]

                    total_count += count

                    if journey_name not in journey_dict:
                        journey_dict[journey_name] = {
                            "journey_name": journey_name,
                            "journey_count": 0,
                            "events": []
                        }

                    journey_dict[journey_name]["journey_count"] += count

                    event_found = False
                    for event in journey_dict[journey_name]["events"]:
                        if event["event_name"] == event_name:
                            event["event_count"] += count
                            event["statuses"].append({
                                "display_name": display_name,
                                "count": count,
                                "journey_id": journey_id,
                                "journey_event_id": journey_event_id,
                                "status_id": status_id
                            })
                            event_found = True
                            break

                    if not event_found:
                        journey_dict[journey_name]["events"].append({
                            "event_name": event_name,
                            "event_count": count,
                            "statuses": [{
                                "display_name": display_name,
                                "count": count,
                                "journey_id": journey_id,
                                "journey_event_id": journey_event_id,
                                "status_id": status_id
                            }]
                        })

                response_data["total_count"] = total_count
                response_data["journeys"] = list(journey_dict.values())

            else:
                # When is_count is 0, flatten the candidates list
                candidates = []
                for row in result:
                    candidate_json = row[4]
                    if candidate_json:
                        candidates.extend(json.loads(candidate_json))

                response_data = {'candidates': candidates}

            # Cache the result for future use
            cache.set(cache_key, response_data, timeout=300)  # Cache timeout is set to 5 minutes (300 seconds)

            return JsonResponse(response_data, safe=False)

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=400)

