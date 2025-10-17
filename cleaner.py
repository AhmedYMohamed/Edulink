import os
import pickle
import json
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

def cleanup_tokens():
    """Remove all authentication token files"""
    token_files = ['token.pickle', 'token.pkl', 'credentials.pkl']
    cleaned = []
    
    for token_file in token_files:
        if os.path.exists(token_file):
            try:
                os.remove(token_file)
                cleaned.append(token_file)
                print(f"✅ Deleted: {token_file}")
            except Exception as e:
                print(f"❌ Could not delete {token_file}: {e}")
        else:
            print(f"ℹ️  File not found: {token_file}")
    
    if cleaned:
        print(f"\n🧹 Cleaned up {len(cleaned)} token file(s)")
    else:
        print("ℹ️  No token files found to clean up")
    
    return len(cleaned) > 0

def test_authentication():
    """Test authentication with minimal scopes first"""
    print("\n🔐 Testing authentication...")
    
    # Start with minimal scopes to test authentication
    MINIMAL_SCOPES = [
        # 'https://www.googleapis.com/auth/classroom.courses.readonly',
        'https://www.googleapis.com/auth/classroom.profile.emails'
    ]
    
    credentials_file = 'credentials.json'
    
    if not os.path.exists(credentials_file):
        print(f"❌ Credentials file '{credentials_file}' not found!")
        return None
    
    try:
        print("Starting OAuth flow with minimal permissions...")
        flow = InstalledAppFlow.from_client_secrets_file(
            credentials_file, MINIMAL_SCOPES)
        
        # Use a specific port to avoid conflicts
        creds = flow.run_local_server(port=8080, prompt='select_account')
        
        # Test the credentials
        service = build('classroom', 'v1', credentials=creds)
        
        # Test API call
        print("Testing API connection...")
        profile = service.userProfiles().get(userId='me').execute()
        
        print(f"✅ Authentication successful!")
        print(f"   User: {profile.get('name', {}).get('fullName', 'Unknown')}")
        print(f"   Email: {profile.get('emailAddress', 'Unknown')}")
        
        # Test courses
        print("\nTesting course access...")
        courses_result = service.courses().list(pageSize=10).execute()
        courses = courses_result.get('courses', [])
        
        print(f"✅ Found {len(courses)} courses")
        if courses:
            for i, course in enumerate(courses[:3], 1):
                print(f"   {i}. {course.get('name', 'Unknown')} ({course.get('courseState', 'Unknown')})")
            if len(courses) > 3:
                print(f"   ... and {len(courses) - 3} more")
        
        return creds, service
        
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return None

def full_authentication():
    """Authenticate with all required scopes"""
    print("\n🔐 Authenticating with full permissions...")
    
    FULL_SCOPES = [
        'https://www.googleapis.com/auth/classroom.courses.readonly',
        'https://www.googleapis.com/auth/classroom.rosters.readonly',
        # 'https://www.googleapis.com/auth/classroom.coursework.students.readonly',
        # 'https://www.googleapis.com/auth/classroom.coursework.me.readonly',
        'https://www.googleapis.com/auth/classroom.announcements.readonly',
        'https://www.googleapis.com/auth/classroom.student-submissions.students.readonly',
        'https://www.googleapis.com/auth/classroom.student-submissions.me.readonly',
        'https://www.googleapis.com/auth/classroom.profile.emails',
        'https://www.googleapis.com/auth/classroom.profile.photos'
    ]
    
    credentials_file = 'credentials.json'
    token_file = 'token_new.pickle'
    
    try:
        print("Starting OAuth flow with full permissions...")
        flow = InstalledAppFlow.from_client_secrets_file(
            credentials_file, FULL_SCOPES)
        
        creds = flow.run_local_server(port=8081, prompt='select_account')
        
        # Save the new token
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
        
        print(f"✅ Full authentication successful!")
        print(f"   Token saved as: {token_file}")
        
        return creds
        
    except Exception as e:
        print(f"❌ Full authentication failed: {e}")
        return None

def quick_data_test(creds):
    """Quick test to fetch some basic data"""
    print("\n📚 Testing data access...")
    
    try:
        service = build('classroom', 'v1', credentials=creds)
        
        # Get courses
        courses_result = service.courses().list(pageSize=5).execute()
        courses = courses_result.get('courses', [])
        
        if not courses:
            print("⚠️  No courses found. This could mean:")
            print("   - You're not enrolled in or teaching any courses")
            print("   - You need to be signed in with a different account")
            return False
        
        print(f"📚 Testing with {len(courses)} course(s):")
        
        for course in courses[:2]:  # Test first 2 courses
            course_id = course['id']
            course_name = course.get('name', 'Unknown')
            
            print(f"\n   Testing course: {course_name}")
            
            # Test students
            try:
                students = service.courses().students().list(courseId=course_id).execute()
                student_count = len(students.get('students', []))
                print(f"     👥 Students: {student_count}")
            except HttpError as e:
                if e.resp.status == 403:
                    print(f"     👥 Students: Permission denied")
                else:
                    print(f"     👥 Students: Error ({e.resp.status})")
            
            # Test assignments
            try:
                assignments = service.courses().courseWork().list(courseId=course_id).execute()
                assignment_count = len(assignments.get('courseWork', []))
                print(f"     📝 Assignments: {assignment_count}")
            except HttpError as e:
                if e.resp.status == 403:
                    print(f"     📝 Assignments: Permission denied")
                else:
                    print(f"     📝 Assignments: Error ({e.resp.status})")
        
        print(f"\n✅ Data access test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Data access test failed: {e}")
        return False

def main():
    print("🧹 Google Classroom API Cleanup & Test Tool")
    print("=" * 50)
    
    # Step 1: Clean up old tokens
    print("Step 1: Cleaning up old authentication tokens...")
    cleanup_tokens()
    
    # Step 2: Test basic authentication
    print("\nStep 2: Testing basic authentication...")
    auth_result = test_authentication()
    
    if not auth_result:
        print("\n❌ Basic authentication failed. Please check:")
        print("1. Your credentials.json file is valid")
        print("2. Google Classroom API is enabled in your project")
        print("3. You're using the correct Google account")
        return
    
    creds, service = auth_result
    
    # Step 3: Test full authentication
    print("\nStep 3: Setting up full permissions...")
    full_creds = full_authentication()
    
    if not full_creds:
        print("\n❌ Full authentication failed.")
        return
    
    # Step 4: Quick data test
    success = quick_data_test(full_creds)
    
    if success:
        print("\n🎉 SUCCESS! Your authentication is working properly.")
        print("\nNext steps:")
        print("1. You can now use 'token_new.pickle' as your token file")
        print("2. Update your main script to use this token file")
        print("3. Or simply rename 'token_new.pickle' to 'token.pickle'")
        
        # Ask if user wants to rename the token
        try:
            rename = input("\nRename 'token_new.pickle' to 'token.pickle'? (y/n): ").strip().lower()
            if rename in ['y', 'yes']:
                if os.path.exists('token.pickle'):
                    os.remove('token.pickle')
                os.rename('token_new.pickle', 'token.pickle')
                print("✅ Token file renamed. You can now run your main script!")
        except KeyboardInterrupt:
            print("\nRename skipped. You can manually rename the file later.")
    else:
        print("\n⚠️  Authentication works, but there may be permission issues.")
        print("Check if you're using the correct Google account with course access.")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()