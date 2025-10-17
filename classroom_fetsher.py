import os
import json
import pickle
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

class ClassroomDataFetcher:
    def __init__(self, credentials_file='credentials.json', token_file='token.pickle', max_workers=10):
        """
        Initialize the Classroom API client with parallel processing
        
        Args:
            credentials_file: Path to your OAuth 2.0 client credentials JSON file
            token_file: Path to store the authentication token
            max_workers: Number of concurrent threads for API requests
        """
        self.SCOPES = [
            'https://www.googleapis.com/auth/classroom.courses.readonly',
            'https://www.googleapis.com/auth/classroom.rosters.readonly',
            'https://www.googleapis.com/auth/classroom.announcements.readonly',
            'https://www.googleapis.com/auth/classroom.student-submissions.students.readonly',
            'https://www.googleapis.com/auth/classroom.student-submissions.me.readonly',
            'https://www.googleapis.com/auth/classroom.profile.emails',
            'https://www.googleapis.com/auth/classroom.profile.photos'
        ]
        
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.max_workers = max_workers
        self.service = None
        self.creds = None
        self._authenticate()
    
    def _authenticate(self):
        """Handle OAuth 2.0 authentication"""
        creds = None
        
        if not os.path.exists(self.credentials_file):
            raise FileNotFoundError(f"Credentials file '{self.credentials_file}' not found.")
        
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'rb') as token:
                    creds = pickle.load(token)
            except Exception as e:
                print(f"Error loading token: {e}")
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception:
                    creds = None
            
            if not creds:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, self.SCOPES)
                creds = flow.run_local_server(port=0)
            
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)
        
        self.creds = creds
        self.service = build('classroom', 'v1', credentials=creds)
        print("API client initialized")
    
    def _get_service(self):
        """Get a new service instance for thread-safe operations"""
        return build('classroom', 'v1', credentials=self.creds)
    
    def _paginated_fetch(self, service, method, **kwargs):
        """Generic paginated fetch with better performance"""
        items = []
        request = method(**kwargs)
        
        while request is not None:
            try:
                response = request.execute()
                items.extend(response.get(list(response.keys())[0], []))
                request = method(**kwargs, pageToken=response.get('nextPageToken'))
                if 'nextPageToken' not in response:
                    break
            except HttpError as e:
                if e.resp.status == 403:
                    print(f"Permission denied: {kwargs}")
                break
            except Exception as e:
                print(f"Error: {e}")
                break
        
        return items
    
    def get_all_courses(self):
        """Fetch all courses"""
        print("Fetching courses...")
        try:
            results = self.service.courses().list(pageSize=100).execute()
            courses = results.get('courses', [])
            
            while 'nextPageToken' in results:
                results = self.service.courses().list(
                    pageSize=100,
                    pageToken=results['nextPageToken']
                ).execute()
                courses.extend(results.get('courses', []))
            
            print(f"Found {len(courses)} courses")
            return courses
        except Exception as e:
            print(f"Error fetching courses: {e}")
            return []
    
    def _fetch_course_data(self, course_id, course_name, include_submissions=True):
        """Fetch all data for a single course using parallel requests"""
        service = self._get_service()
        
        course_data = {
            'students': [],
            'teachers': [],
            'assignments': [],
            'announcements': []
        }
        
        # Fetch basic course data in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                'students': executor.submit(self._fetch_students, service, course_id),
                'teachers': executor.submit(self._fetch_teachers, service, course_id),
                'assignments': executor.submit(self._fetch_assignments, service, course_id),
                'announcements': executor.submit(self._fetch_announcements, service, course_id)
            }
            
            for key, future in futures.items():
                try:
                    course_data[key] = future.result()
                except Exception as e:
                    print(f"  Error fetching {key}: {e}")
        
        print(f"  Students: {len(course_data['students'])}, Teachers: {len(course_data['teachers'])}, "
              f"Assignments: {len(course_data['assignments'])}, Announcements: {len(course_data['announcements'])}")
        
        # Fetch submissions in parallel if requested
        if include_submissions and course_data['assignments']:
            print(f"  Fetching submissions for {len(course_data['assignments'])} assignments...")
            course_data['assignments'] = self._fetch_all_submissions(
                service, course_id, course_data['assignments']
            )
        
        return course_data
    
    def _fetch_students(self, service, course_id):
        """Fetch students for a course"""
        try:
            results = service.courses().students().list(courseId=course_id, pageSize=100).execute()
            students = results.get('students', [])
            
            while 'nextPageToken' in results:
                results = service.courses().students().list(
                    courseId=course_id,
                    pageSize=100,
                    pageToken=results['nextPageToken']
                ).execute()
                students.extend(results.get('students', []))
            
            return students
        except Exception:
            return []
    
    def _fetch_teachers(self, service, course_id):
        """Fetch teachers for a course"""
        try:
            results = service.courses().teachers().list(courseId=course_id, pageSize=100).execute()
            teachers = results.get('teachers', [])
            
            while 'nextPageToken' in results:
                results = service.courses().teachers().list(
                    courseId=course_id,
                    pageSize=100,
                    pageToken=results['nextPageToken']
                ).execute()
                teachers.extend(results.get('teachers', []))
            
            return teachers
        except Exception:
            return []
    
    def _fetch_assignments(self, service, course_id):
        """Fetch assignments for a course"""
        try:
            results = service.courses().courseWork().list(courseId=course_id, pageSize=100).execute()
            assignments = results.get('courseWork', [])
            
            while 'nextPageToken' in results:
                results = service.courses().courseWork().list(
                    courseId=course_id,
                    pageSize=100,
                    pageToken=results['nextPageToken']
                ).execute()
                assignments.extend(results.get('courseWork', []))
            
            return assignments
        except Exception:
            return []
    
    def _fetch_announcements(self, service, course_id):
        """Fetch announcements for a course"""
        try:
            results = service.courses().announcements().list(courseId=course_id, pageSize=100).execute()
            announcements = results.get('announcements', [])
            
            while 'nextPageToken' in results:
                results = service.courses().announcements().list(
                    courseId=course_id,
                    pageSize=100,
                    pageToken=results['nextPageToken']
                ).execute()
                announcements.extend(results.get('announcements', []))
            
            return announcements
        except Exception:
            return []
    
    def _fetch_single_assignment_submissions(self, service, course_id, assignment):
        """Fetch submissions for a single assignment"""
        assignment_id = assignment['id']
        try:
            results = service.courses().courseWork().studentSubmissions().list(
                courseId=course_id,
                courseWorkId=assignment_id,
                pageSize=100
            ).execute()
            submissions = results.get('studentSubmissions', [])
            
            while 'nextPageToken' in results:
                results = service.courses().courseWork().studentSubmissions().list(
                    courseId=course_id,
                    courseWorkId=assignment_id,
                    pageSize=100,
                    pageToken=results['nextPageToken']
                ).execute()
                submissions.extend(results.get('studentSubmissions', []))
            
            assignment['submissions'] = submissions
            return assignment
        except Exception:
            assignment['submissions'] = []
            return assignment
    
    def _fetch_all_submissions(self, service, course_id, assignments):
        """Fetch submissions for all assignments in parallel"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._fetch_single_assignment_submissions, 
                              self._get_service(), course_id, assignment)
                for assignment in assignments
            ]
            
            updated_assignments = []
            for future in as_completed(futures):
                try:
                    updated_assignments.append(future.result())
                except Exception as e:
                    print(f"    Error fetching submissions: {e}")
        
        return updated_assignments
    
    def fetch_all_data(self, output_dir='classroom_data', include_submissions=True):
        """Fetch all data from Google Classroom using parallel processing"""
        print("=" * 50)
        print("OPTIMIZED GOOGLE CLASSROOM DATA EXPORT")
        print("=" * 50)
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        all_data = {
            'export_timestamp': datetime.now().isoformat(),
            'courses': []
        }
        
        courses = self.get_all_courses()
        
        if not courses:
            print("No courses found.")
            return all_data
        
        # Process courses in parallel
        print(f"\nProcessing {len(courses)} courses in parallel...")
        
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(courses))) as executor:
            futures = {
                executor.submit(
                    self._fetch_course_data,
                    course['id'],
                    course.get('name', 'Unknown'),
                    include_submissions
                ): course for course in courses
            }
            
            for i, future in enumerate(as_completed(futures), 1):
                course = futures[future]
                course_name = course.get('name', 'Unknown')
                print(f"\n[{i}/{len(courses)}] Processing: {course_name}")
                
                try:
                    course_data = future.result()
                    all_data['courses'].append({
                        'course_info': course,
                        **course_data
                    })
                except Exception as e:
                    print(f"  Error processing course: {e}")
        
        # Save data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f'classroom_data_{timestamp}.json')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n{'=' * 50}")
        print(f"Export completed! Saved to: {output_file}")
        print(f"File size: {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")
        print(f"{'=' * 50}")
        
        return all_data


def main():
    """Main function"""
    print("Optimized Google Classroom Data Fetcher")
    print("=" * 50)
    
    # Initialize with parallel processing (10 concurrent workers)
    fetcher = ClassroomDataFetcher(max_workers=10)
    
    # Fetch all data with submissions
    data = fetcher.fetch_all_data(include_submissions=True)


if __name__ == '__main__':
    main()