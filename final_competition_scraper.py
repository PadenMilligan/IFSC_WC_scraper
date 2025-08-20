import requests
import pandas as pd
import time
from datetime import datetime
import json
import re

class FinalIFSCCompetitionScraper:
    def __init__(self):
        self.base_url = "https://ifsc.results.info"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://ifsc.results.info/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
        })
        self.results = []
        
    def get_api_data(self, endpoint):
        """Get data from an API endpoint"""
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def get_season_data(self, year):
        """Get season data for a specific year"""
        print(f"Fetching {year} season data...")
        season_data = self.get_api_data("/api/v1/")
        
        if not season_data:
            return None
        
        # Find the specified season
        for season in season_data.get('seasons', []):
            if season.get('name') == str(year):
                print(f"Found {year} season with ID: {season.get('id')}")
                return season
        
        return None
    
    def get_world_cup_league(self, season_data):
        """Get the World Cups and World Championships league"""
        for league in season_data.get('leagues', []):
            if 'World Cups and World Championships' in league.get('name', ''):
                print(f"Found World Cup league: {league.get('name')}")
                return league
        return None
    
    def get_league_events(self, league):
        """Get events from the league"""
        league_url = league.get('url')
        if not league_url:
            print("No league URL found")
            return []
        
        print(f"Getting league events from: {league_url}")
        league_data = self.get_api_data(league_url)
        
        if not league_data:
            return []
        
        events = league_data.get('events', [])
        print(f"Found {len(events)} events in league")
        return events
    
    def get_event_results(self, event):
        """Get results from a specific event"""
        event_name = event.get('event', 'Unknown Event')
        event_id = event.get('event_id')
        
        print(f"\n  Processing event: {event_name}")
        print(f"    Event ID: {event_id}")
        print(f"    Date: {event.get('local_start_date', 'Unknown')} - {event.get('local_end_date', 'Unknown')}")
        
        # Get event details
        event_detail_url = f"/api/v1/events/{event_id}"
        event_data = self.get_api_data(event_detail_url)
        
        if not event_data:
            print(f"    Failed to get event details")
            return []
        
        # Process discipline categories
        d_cats = event_data.get('d_cats', [])
        print(f"    Discipline categories: {len(d_cats)}")
        
        all_results = []
        for d_cat in d_cats:
            # Use the correct fields for discipline and gender
            discipline = d_cat.get('discipline_kind', 'Unknown').title()
            gender = d_cat.get('category_name', 'Unknown')
            
            print(f"      Processing {discipline} - {gender}")
            
            # Process each round
            category_rounds = d_cat.get('category_rounds', [])
            for round_data in category_rounds:
                round_name = round_data.get('name', '')
                round_type = self.extract_round_type(round_name)
                
                # Only process finals and semifinals
                if round_type not in ['Final', 'Semifinal']:
                    continue
                
                print(f"        Processing {round_type}: {round_name}")
                
                # Get round results
                round_id = round_data.get('category_round_id')
                if round_id:
                    round_results = self.get_api_data(f"/api/v1/category_rounds/{round_id}/results")
                    if round_results:
                        results = self.process_round_results(
                            round_results, 
                            event_name, 
                            discipline, 
                            gender, 
                            round_type,
                            round_name,
                            event.get('local_start_date', ''),
                            event.get('local_end_date', '')
                        )
                        all_results.extend(results)
                        print(f"          Collected {len(results)} results from {round_type}")
                    
                    time.sleep(0.5)  # Be polite
        
        return all_results
    
    def process_round_results(self, round_results, event_name, discipline, gender, round_type, round_name, start_date, end_date):
        """Process results from a specific round"""
        results = []
        
        # Extract year from event name
        year_match = re.search(r'\b(20\d{2})\b', event_name)
        year = year_match.group(1) if year_match else 'Unknown'
        
        # Process the results
        if isinstance(round_results, dict):
            # Look for results in different possible keys
            results_data = round_results.get('results', [])
            if not results_data:
                results_data = round_results.get('ranking', [])
            if not results_data:
                results_data = round_results.get('data', [])
            
            if results_data and isinstance(results_data, list):
                for position, athlete in enumerate(results_data, 1):
                    if isinstance(athlete, dict):
                        # Extract athlete information
                        first_name = athlete.get('firstname', athlete.get('first_name', ''))
                        last_name = athlete.get('lastname', athlete.get('last_name', ''))
                        athlete_name = f"{first_name} {last_name}".strip()
                        
                        if not athlete_name:
                            continue
                        
                        result = {
                            'year': year,
                            'event': event_name,
                            'discipline': discipline,
                            'gender': gender,
                            'round_name': round_name,
                            'rank': athlete.get('rank', athlete.get('position', position)),
                            'athlete': athlete_name,
                            'country': athlete.get('country', athlete.get('country_code', '')),
                            'score': athlete.get('score', athlete.get('result', '')),
                            'points': athlete.get('points', ''),
                            'start_date': start_date,
                            'end_date': end_date,
                            'scraped_at': datetime.now().isoformat()
                        }
                        
                        results.append(result)
        
        return results
    
    def extract_discipline(self, category_name):
        """Extract discipline from category name"""
        category_lower = category_name.lower()
        
        if 'boulder' in category_lower:
            return 'Boulder'
        elif 'lead' in category_lower:
            return 'Lead'
        elif 'speed' in category_lower:
            return 'Speed'
        elif 'combined' in category_lower:
            return 'Combined'
        elif 'boulder&lead' in category_lower or 'boulder & lead' in category_lower:
            return 'Boulder & Lead'
        else:
            return 'Unknown'
    
    def extract_gender(self, category_name):
        """Extract gender from category name"""
        category_lower = category_name.lower()
        
        if 'women' in category_lower or 'female' in category_lower:
            return 'Women'
        elif 'men' in category_lower or 'male' in category_lower:
            return 'Men'
        else:
            return 'Unknown'
    
    def extract_round_type(self, round_name):
        """Extract round type from round name"""
        round_lower = round_name.lower()
        
        if 'semi' in round_lower or 'semi-final' in round_lower:
            return 'Semifinal'
        elif 'final' in round_lower:
            return 'Final'
        elif 'qualification' in round_lower or 'qual' in round_lower:
            return 'Qualification'
        else:
            return 'Other'
    
    def scrape_season_data(self, year):
        """Scrape competition data for a specific season"""
        print(f"\nScraping {year} season competition data...")
        print("="*60)
        
        # Get season data
        season_data = self.get_season_data(year)
        if not season_data:
            print(f"Could not find {year} season data")
            return []
        
        # Get World Cup league
        world_cup_league = self.get_world_cup_league(season_data)
        if not world_cup_league:
            print(f"Could not find World Cup league for {year}")
            return []
        
        # Get events from the league
        events = self.get_league_events(world_cup_league)
        if not events:
            print(f"No events found for {year}")
            return []
        
        # Get competition results from each event
        all_results = []
        for event in events:
            results = self.get_event_results(event)
            all_results.extend(results)
            print(f"  Total results from {event.get('event', 'Unknown')}: {len(results)}")
        
        return all_results
    
    def scrape_all_available_data(self):
        """Scrape all available competition data from recent seasons"""
        print("Starting IFSC Competition Results Scraper...")
        print("="*60)
        print("This scraper will collect ACTUAL COMPETITION RESULTS (not rankings)")
        print("Looking for events like 'IFSC World Cup Keqiao 2025' with finals/semifinals")
        print("="*60)
        
        # Try to get data from recent years
        years_to_try = [2025, 2024, 2023]
        all_results = []
        
        for year in years_to_try:
            try:
                results = self.scrape_season_data(year)
                if results:
                    all_results.extend(results)
                    print(f"Successfully collected {len(results)} competition results from {year}")
                else:
                    print(f"No competition results available for {year}")
            except Exception as e:
                print(f"Error scraping {year}: {e}")
                continue
        
        self.results = all_results
        return all_results
    
    def save_to_csv(self, filename="ifsc_competition_results.csv"):
        """Save results to CSV file"""
        if not self.results:
            print("No results to save")
            return None
        
        df = pd.DataFrame(self.results)
        
        # Clean up the data
        df = df.dropna(subset=['athlete'])  # Remove rows without athlete names
        
        # Reorder columns for better readability
        preferred_columns = [
            'year', 'event', 'discipline', 'gender', 'round_name',
            'rank', 'athlete', 'country', 'score', 'points', 'start_date', 'end_date'
        ]
        available_columns = [col for col in preferred_columns if col in df.columns]
        other_columns = [col for col in df.columns if col not in preferred_columns]
        df = df[available_columns + other_columns]
        
        # Save to CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Saved {len(df)} competition results to {filename}")
        
        # Also save a summary
        summary_filename = "ifsc_competition_summary.txt"
        with open(summary_filename, 'w', encoding='utf-8') as f:
            f.write(f"IFSC Competition Results Scraper Summary\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write(f"Total competition results: {len(df)}\n\n")
            
            f.write("Results by Year:\n")
            year_counts = df['year'].value_counts()
            for year, count in year_counts.items():
                f.write(f"  {year}: {count}\n")
            
            f.write("\nResults by Gender:\n")
            gender_counts = df['gender'].value_counts()
            for gender, count in gender_counts.items():
                f.write(f"  {gender}: {count}\n")
            
            f.write("\nResults by Discipline:\n")
            discipline_counts = df['discipline'].value_counts()
            for discipline, count in discipline_counts.items():
                f.write(f"  {discipline}: {count}\n")
            
            f.write("\nResults by Round Name:\n")
            round_name_counts = df['round_name'].value_counts()
            for round_name, count in round_name_counts.items():
                f.write(f"  {round_name}: {count}\n")
            
            f.write("\nResults by Event:\n")
            event_counts = df['event'].value_counts()
            for event, count in event_counts.items():
                f.write(f"  {event}: {count}\n")
        
        print(f"Saved summary to {summary_filename}")
        return df

def main():
    """Main function to run the competition scraper"""
    scraper = FinalIFSCCompetitionScraper()
    
    try:
        # Scrape all available competition data
        results = scraper.scrape_all_available_data()
        
        if results:
            # Save results to CSV
            df = scraper.save_to_csv()
            
            # Display some statistics
            print("\n" + "="*60)
            print("COMPETITION SCRAPING COMPLETED")
            print("="*60)
            print(f"Total competition results collected: {len(results)}")
            
            if df is not None:
                print(f"\nCompetition results breakdown:")
                print(f"Years: {df['year'].value_counts().to_dict()}")
                print(f"Genders: {df['gender'].value_counts().to_dict()}")
                print(f"Disciplines: {df['discipline'].value_counts().to_dict()}")
                print(f"Events: {len(df['event'].unique())}")
                
                # Show some example competitions
                print(f"\nExample competitions found:")
                events = df['event'].unique()
                for event in events[:5]:  # Show first 5
                    print(f"  - {event}")
        else:
            print("No competition results were collected.")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 