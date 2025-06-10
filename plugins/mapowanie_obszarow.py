import pandas as pd

community_to_area = {
    # Foothill
    'Tujunga': 'Foothill',
    'Sunland': 'Foothill',
    'Shadow Hills': 'Foothill',
    'Lake View Terrace': 'Foothill',
    'Kagel/Lopez Canyons': 'Foothill',
    
    # Mission
    'Pacoima': 'Mission',
    'Sylmar': 'Mission',
    'Arleta': 'Mission',
    'Panorama City': 'Mission',
    'Mission Hills': 'Mission',
    'North Hills': 'Mission',
    
    # Devonshire
    'Granada Hills': 'Devonshire',
    'Porter Ranch': 'Devonshire',
    'Northridge': 'Devonshire',
    'Chatsworth': 'Devonshire',
    
    # Topanga
    'West Hills': 'Topanga',
    'Canoga Park': 'Topanga',
    'Woodland Hills': 'Topanga',
    'Winnetka': 'Topanga',
    
    # West Valley
    'Tarzana': 'West Valley',
    'Reseda': 'West Valley',
    'Lake Balboa': 'West Valley',
    'Encino': 'West Valley',
    
    # N Hollywood
    'North Hollywood': 'N Hollywood',
    'Valley Village': 'N Hollywood',
    'Valley Glen': 'N Hollywood',
    'Studio City': 'N Hollywood',
    'Toluca Lake': 'N Hollywood',
    'Sherman Oaks': 'N Hollywood',
    
    # Van Nuys
    'Van Nuys': 'Van Nuys',
    
    # Northeast
    'Eagle Rock': 'Northeast',
    'Highland Park': 'Northeast',
    'Mount Washington': 'Northeast',
    'Glassell Park': 'Northeast',
    'Cypress Park': 'Northeast',
    'Atwater Village': 'Northeast',
    'El Sereno': 'Northeast',
    'Montecito Heights': 'Northeast',
    'Lincoln Heights': 'Northeast',
    
    # Hollywood
    'Hollywood': 'Hollywood',
    'Hollywood Hills': 'Hollywood',
    'East Hollywood': 'Hollywood',
    'Los Feliz': 'Hollywood',
    'Silver Lake': 'Hollywood',
    
    # Rampart
    'Echo Park': 'Rampart',
    'Westlake': 'Rampart',
    'Pico-Union': 'Rampart',
    'Koreatown': 'Rampart',
    
    # Olympic
    'Mid-Wilshire': 'Olympic',
    'Wilshire': 'Olympic',
    'Fairfax': 'Olympic',
    'Larchmont': 'Olympic',
    'Hancock Park': 'Olympic',
    
    # Central
    'Downtown': 'Central',
    'Chinatown': 'Central',
    
    # Hollenbeck
    'Boyle Heights': 'Hollenbeck',
    
    # Southwest
    'Exposition Park': 'Southwest',
    'University Park': 'Southwest',
    'Adams-Normandie': 'Southwest',
    'Jefferson Park': 'Southwest',
    'Leimert Park': 'Southwest',
    'Baldwin Hills': 'Southwest',
    'Crenshaw': 'Southwest',
    
    # Southeast
    'Watts': 'Southeast',
    'Green Meadows': 'Southeast',
    'Florence': 'Southeast',
    'Vermont Vista': 'Southeast',
    'Broadway-Manchester': 'Southeast',
    
    # 77th Street
    'Vermont-Slauson': '77th Street',
    'Harvard Park': '77th Street',
    'Chesterfield Square': '77th Street',
    'Vermont Knolls': '77th Street',
    'Manchester Square': '77th Street',
    'Gramercy Park': '77th Street',
    
    # West LA
    'Brentwood': 'West LA',
    'Westwood': 'West LA',
    'Bel-Air': 'West LA',
    'Pacific Palisades': 'West LA',
    
    # Pacific
    'Venice': 'Pacific',
    'Mar Vista': 'Pacific',
    'Playa Vista': 'Pacific',
    'Del Rey': 'Pacific',
    'Westchester': 'Pacific',
    'Palms': 'Pacific',
    
    # Harbor
    'San Pedro': 'Harbor',
    'Wilmington': 'Harbor',
    'Harbor City': 'Harbor',
    'Harbor Gateway': 'Harbor'
  }