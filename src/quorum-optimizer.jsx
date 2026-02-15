import React, { useState, useEffect } from 'react';

const QuorumOptimizer = () => {
  const [activeTab, setActiveTab] = useState('location-visits');
  const [agencies, setAgencies] = useState([]);
  const [advertisers, setAdvertisers] = useState([]);
  const [selectedAgency, setSelectedAgency] = useState('');
  const [selectedAdvertiser, setSelectedAdvertiser] = useState('');
  const [agencySearch, setAgencySearch] = useState('');
  const [expandedAgency, setExpandedAgency] = useState('');
  const [dateRange, setDateRange] = useState({ start: '2025-12-01', end: '2026-01-17' });
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState(null);
  const [copied, setCopied] = useState('');
  const [showMethodology, setShowMethodology] = useState(false);
  
  // Sidebar resize state
  const [sidebarWidth, setSidebarWidth] = useState(320);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [isResizing, setIsResizing] = useState(false);
  
  const minSidebarWidth = 240;
  const maxSidebarWidth = 450;
  const collapsedWidth = 48;
  
  const handleMouseDown = (e) => {
    e.preventDefault();
    setIsResizing(true);
  };
  
  useEffect(() => {
    const handleMouseMove = (e) => {
      if (!isResizing) return;
      const newWidth = Math.min(Math.max(e.clientX, minSidebarWidth), maxSidebarWidth);
      setSidebarWidth(newWidth);
    };
    const handleMouseUp = () => setIsResizing(false);
    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    }
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isResizing]);
  
  // ============================================================
  // AGENCY & ADVERTISER CONFIG
  // ============================================================
  const realAgencies = [
    ['2514', 'MNTN', 'Store'],
    ['2234', 'Magnite', 'Store'],
    ['1813', 'Causal iQ', 'Store'],
    ['1480', 'ViacomCBS WhoSay', 'Web']
  ];
  
  const realAdvertisers = {
    '2514': [['45143', 'Noodles & Company'], ['27828', 'First Watch'], ['37704', 'Sky Zone'], ['45346', 'Northern Tool'], ['32623', 'Visit PA']],
    '2234': [['40143', 'Mountain West Bank'], ['39323', 'Visit Pensacola']],
    '1813': [['45141', 'Lowes TTD'], ['8123', 'AutoZone'], ['27588', 'Golden Nugget Casino'], ['7389', 'Space Coast Tourism']],
    '1480': [['40514', 'Lean RX']]
  };

  // ============================================================
  // ADVERTISER DATA STORE
  // ============================================================
  const advertiserData = {
    // MNTN - Noodles & Company
    '45143': {
      summary: { totalImpressions: 7624633, totalVisits: 30157, overallVisitRate: 0.00395, liftVsControl: 18.3 },
      campaigns: [
        { name: '2025 - Priority - Denver', impressions: 1222663, visits: 8661, visitRate: 0.007084 },
        { name: '2025 - Priority - Chicago', impressions: 1183200, visits: 3249, visitRate: 0.002746 },
        { name: '2025 - Priority - Minneapolis-StPaul', impressions: 679792, visits: 4855, visitRate: 0.007142 },
        { name: '2025 - Priority - Retargeting', impressions: 639548, visits: 1688, visitRate: 0.002639 },
        { name: '2025 - Secondary - Washington DC', impressions: 546210, visits: 961, visitRate: 0.001759 },
        { name: '2025 - Secondary - Wisconsin', impressions: 526563, visits: 6223, visitRate: 0.011818 },
        { name: '2025 - Secondary - Baltimore', impressions: 334246, visits: 78, visitRate: 0.000233 },
        { name: '2025 - Remaining Markets', impressions: 319002, visits: 1915, visitRate: 0.006003 }
      ],
      publishers: [
        { code: 'HBO Max', impressions: 1974033, visits: 922, visitRate: 0.000467 },
        { code: 'NBC', impressions: 1872428, visits: 938, visitRate: 0.000501 },
        { code: 'Paramount Streaming - Comedy', impressions: 1274774, visits: 633, visitRate: 0.000497 },
        { code: 'Paramount Streaming - Drama', impressions: 1185666, visits: 576, visitRate: 0.000486 },
        { code: 'Paramount Streaming - News', impressions: 737080, visits: 452, visitRate: 0.000613 },
        { code: 'Paramount Streaming - Action-Thriller', impressions: 640642, visits: 325, visitRate: 0.000507 },
        { code: 'Paramount Streaming - Reality', impressions: 502100, visits: 273, visitRate: 0.000544 },
        { code: 'Tubi Entertainment', impressions: 475965, visits: 188, visitRate: 0.000395 },
        { code: 'TLC', impressions: 324575, visits: 93, visitRate: 0.000287 },
        { code: 'Paramount Streaming - Sci-Fi', impressions: 321660, visits: 190, visitRate: 0.000591 },
        { code: 'Paramount Streaming - Documentary', impressions: 246090, visits: 168, visitRate: 0.000683 },
        { code: 'Paramount Streaming - Sports', impressions: 221269, visits: 198, visitRate: 0.000895 },
        { code: 'Discovery Channel', impressions: 138923, visits: 110, visitRate: 0.000792 }
      ],
      zipCodes: [
        { zip: '80208', dma: 'DENVER', population: 26592, impressions: 263564, visits: 1892 },
        { zip: '55440', dma: 'MINNEAPOLIS - ST. PAUL', population: 8996, impressions: 231766, visits: 1583 },
        { zip: '60666', dma: 'CHICAGO', population: 9926, impressions: 217402, visits: 474 },
        { zip: '20068', dma: 'WASHINGTON, DC', population: 2207, impressions: 171308, visits: 267 },
        { zip: '50981', dma: 'DES MOINES - AMES', population: 2026, impressions: 142151, visits: 0 },
        { zip: '21215', dma: 'BALTIMORE', population: 58448, impressions: 135857, visits: 41 },
        { zip: '80001', dma: 'DENVER', population: 22, impressions: 131503, visits: 798 },
        { zip: '21203', dma: 'BALTIMORE', population: 21010, impressions: 114418, visits: 6 },
        { zip: '20147', dma: 'WASHINGTON, DC', population: 64197, impressions: 110484, visits: 235 },
        { zip: '60126', dma: 'CHICAGO', population: 48410, impressions: 107495, visits: 226 },
        { zip: '80040', dma: 'DENVER', population: 25323, impressions: 93129, visits: 483 },
        { zip: '60701', dma: 'CHICAGO', population: 9926, impressions: 93061, visits: 61 },
        { zip: '80160', dma: 'DENVER', population: 33603, impressions: 90229, visits: 591 },
        { zip: '60056', dma: 'CHICAGO', population: 55250, impressions: 81795, visits: 178 },
        { zip: '80523', dma: 'DENVER', population: 37076, impressions: 72863, visits: 377 }
      ],
      dmaZipCounts: { 'CHICAGO': 362, 'DENVER': 237, 'MINNEAPOLIS - ST. PAUL': 247, 'WASHINGTON, DC': 294, 'BALTIMORE': 117, 'DES MOINES - AMES': 51 }
    },

    // Magnite - Mountain West Bank
    '40143': {
      summary: { totalImpressions: 3442122, totalVisits: 11220, overallVisitRate: 0.00326, liftVsControl: 22.1 },
      campaigns: [
        { name: 'Mountain West Bank 2025 - Aug-Dec', impressions: 3442120, visits: 11220, visitRate: 0.00326 }
      ],
      publishers: [
        { code: 'Samsung Ads', impressions: 593518, visits: 1936, visitRate: 0.00326 },
        { code: 'Paramount Global', impressions: 525474, visits: 1714, visitRate: 0.00326 },
        { code: 'Warner Bros. Discovery', impressions: 419106, visits: 1367, visitRate: 0.00326 },
        { code: 'Sling TV', impressions: 351541, visits: 1147, visitRate: 0.00326 },
        { code: 'Fox', impressions: 233903, visits: 763, visitRate: 0.00326 },
        { code: 'LG Ads', impressions: 236179, visits: 770, visitRate: 0.00326 },
        { code: 'DIRECTV', impressions: 103702, visits: 338, visitRate: 0.00326 },
        { code: 'DISH Connected', impressions: 90373, visits: 295, visitRate: 0.00326 },
        { code: 'TubiTV', impressions: 101634, visits: 331, visitRate: 0.00326 },
        { code: 'A&E', impressions: 46121, visits: 150, visitRate: 0.00326 },
        { code: 'AMC', impressions: 32762, visits: 107, visitRate: 0.00326 }
      ],
      zipCodes: [
        { zip: '83707', dma: 'BOISE', population: 32855, impressions: 555637, visits: 3042 },
        { zip: '83686', dma: 'BOISE', population: 47628, impressions: 346024, visits: 669 },
        { zip: '83816', dma: 'SPOKANE', population: 34141, impressions: 339557, visits: 953 },
        { zip: '83815', dma: 'SPOKANE', population: 34141, impressions: 286463, visits: 1194 },
        { zip: '83704', dma: 'BOISE', population: 40385, impressions: 194580, visits: 429 },
        { zip: '83844', dma: 'SPOKANE', population: 1906, impressions: 160427, visits: 1444 },
        { zip: '68197', dma: 'OMAHA', population: 1526, impressions: 157560, visits: 229 },
        { zip: '83805', dma: 'SPOKANE', population: 8075, impressions: 104034, visits: 664 },
        { zip: '83854', dma: 'SPOKANE', population: 40905, impressions: 93565, visits: 305 },
        { zip: '83714', dma: 'BOISE', population: 21763, impressions: 59969, visits: 20 },
        { zip: '83313', dma: 'TWIN FALLS', population: 3789, impressions: 55734, visits: 164 },
        { zip: '83861', dma: 'SPOKANE', population: 6426, impressions: 48658, visits: 481 }
      ],
      dmaZipCounts: { 'BOISE': 89, 'SPOKANE': 67, 'TWIN FALLS': 23, 'OMAHA': 12 }
    },

    // Magnite - Visit Pensacola
    '39323': {
      summary: { totalImpressions: 1475798, totalVisits: 10516, overallVisitRate: 0.007126, liftVsControl: 31.2 },
      campaigns: [
        { name: 'Live Sports - Aug 25 - Jan 26', impressions: 769918, visits: 7314, visitRate: 0.0095 },
        { name: 'Live Sports - Oct 25 - Jan 26', impressions: 705880, visits: 3202, visitRate: 0.004536 }
      ],
      publishers: [
        { code: 'DIRECTV', impressions: 869120, visits: 6193, visitRate: 0.007126 },
        { code: 'Sling TV', impressions: 356455, visits: 2540, visitRate: 0.007126 },
        { code: 'fubo.TV', impressions: 149091, visits: 1062, visitRate: 0.007126 },
        { code: 'Telly', impressions: 126965, visits: 905, visitRate: 0.007126 },
        { code: 'Flosports', impressions: 8125, visits: 58, visitRate: 0.007126 },
        { code: 'Charter Spectrum', impressions: 5458, visits: 39, visitRate: 0.007126 }
      ],
      zipCodes: [
        { zip: '30302', dma: 'ATLANTA', population: 28077, impressions: 45292, visits: 690 },
        { zip: '75270', dma: 'DALLAS - FT. WORTH', population: 0, impressions: 22769, visits: 116 },
        { zip: '20163', dma: 'WASHINGTON, DC', population: 4448, impressions: 13032, visits: 39 },
        { zip: '37222', dma: 'NASHVILLE', population: 40425, impressions: 10451, visits: 49 },
        { zip: '77052', dma: 'HOUSTON', population: 17052, impressions: 10175, visits: 21 },
        { zip: '10123', dma: 'NEW YORK', population: 0, impressions: 9643, visits: 915 },
        { zip: '70801', dma: 'BATON ROUGE', population: 85, impressions: 7289, visits: 89 },
        { zip: '39157', dma: 'JACKSON, MS', population: 24522, impressions: 6967, visits: 405 },
        { zip: '60666', dma: 'CHICAGO', population: 9926, impressions: 6685, visits: 37 },
        { zip: '30087', dma: 'ATLANTA', population: 40003, impressions: 6590, visits: 51 }
      ],
      dmaZipCounts: { 'ATLANTA': 45, 'DALLAS - FT. WORTH': 38, 'HOUSTON': 32, 'NEW YORK': 28, 'NASHVILLE': 22 }
    },

    // MNTN - First Watch (53.7M imps, 122K visits, 0.23%)
    '27828': {
      summary: { totalImpressions: 53674711, totalVisits: 122629, overallVisitRate: 0.00229, liftVsControl: 28.5 },
      campaigns: [
        { id: 'fw1', name: 'First Watch - Southeast', impressions: 18500000, visits: 45200, visitRate: 0.00244 },
        { id: 'fw2', name: 'First Watch - Midwest', impressions: 15200000, visits: 38100, visitRate: 0.00251 },
        { id: 'fw3', name: 'First Watch - Southwest', impressions: 12800000, visits: 28400, visitRate: 0.00222 },
        { id: 'fw4', name: 'First Watch - Northeast', impressions: 7174711, visits: 10929, visitRate: 0.00152 }
      ],
      publishers: [
        { code: 'Samsung TV+ Entertainment', impressions: 83695, visits: 121018, visitRate: 0.1446 },
        { code: 'Samsung TV+ Reality', impressions: 38619, visits: 55324, visitRate: 0.1432 },
        { code: 'Samsung TV+ News', impressions: 37551, visits: 52215, visitRate: 0.1391 },
        { code: 'Vizio Documentary', impressions: 31925, visits: 45727, visitRate: 0.1432 },
        { code: 'Paramount Streaming - Drama', impressions: 30156, visits: 41675, visitRate: 0.1382 },
        { code: 'Paramount Streaming - Comedy', impressions: 27870, visits: 38702, visitRate: 0.1389 },
        { code: 'Tubi Entertainment', impressions: 23505, visits: 32942, visitRate: 0.1401 },
        { code: 'Roku News', impressions: 21926, visits: 30779, visitRate: 0.1404 }
      ],
      zipCodes: [
        { zip: '85001', dma: 'PHOENIX (PRESCOTT)', population: 52100, impressions: 175675, visits: 33466 },
        { zip: '32802', dma: 'ORLANDO - DAYTONA BCH - MELBRN', population: 38500, impressions: 44289, visits: 6586 },
        { zip: '33606', dma: 'TAMPA - ST. PETE (SARASOTA)', population: 28400, impressions: 23594, visits: 3312 },
        { zip: '33102', dma: 'MIAMI - FT. LAUDERDALE', population: 41200, impressions: 15913, visits: 1386 },
        { zip: '85285', dma: 'PHOENIX (PRESCOTT)', population: 18500, impressions: 13718, visits: 344 },
        { zip: '32256', dma: 'JACKSONVILLE', population: 32100, impressions: 12891, visits: 1426 },
        { zip: '33511', dma: 'TAMPA - ST. PETE (SARASOTA)', population: 24600, impressions: 11583, visits: 1525 },
        { zip: '33074', dma: 'MIAMI - FT. LAUDERDALE', population: 19800, impressions: 11169, visits: 1948 }
      ],
      dmaZipCounts: { 'PHOENIX (PRESCOTT)': 145, 'ORLANDO - DAYTONA BCH - MELBRN': 112, 'TAMPA - ST. PETE (SARASOTA)': 125, 'MIAMI - FT. LAUDERDALE': 156, 'JACKSONVILLE': 76 }
    },

    // MNTN - Sky Zone (30.7M imps, 89K visits, 0.29%)
    '37704': {
      summary: { totalImpressions: 30723733, totalVisits: 89377, overallVisitRate: 0.00291, liftVsControl: 35.2 },
      campaigns: [
        { id: 'sz1', name: 'Sky Zone - Family Weekend', impressions: 12500000, visits: 41250, visitRate: 0.00330 },
        { id: 'sz2', name: 'Sky Zone - Birthday Parties', impressions: 9800000, visits: 28420, visitRate: 0.00290 },
        { id: 'sz3', name: 'Sky Zone - Summer Promo', impressions: 5423733, visits: 13017, visitRate: 0.00240 },
        { id: 'sz4', name: 'Sky Zone - Retargeting', impressions: 3000000, visits: 6690, visitRate: 0.00223 }
      ],
      publishers: [
        { code: 'Tubi Entertainment', impressions: 20694, visits: 35254, visitRate: 0.1703 },
        { code: 'Roku News', impressions: 10676, visits: 18343, visitRate: 0.1718 },
        { code: 'Paramount Streaming - Comedy', impressions: 8720, visits: 15055, visitRate: 0.1727 },
        { code: 'Samsung TV+ Entertainment', impressions: 8366, visits: 13964, visitRate: 0.1669 },
        { code: 'Paramount Streaming - Drama', impressions: 6570, visits: 11148, visitRate: 0.1697 },
        { code: 'Paramount Streaming - Action-Thriller', impressions: 6473, visits: 10988, visitRate: 0.1698 },
        { code: 'Vizio Documentary', impressions: 5328, visits: 9740, visitRate: 0.1828 },
        { code: 'Roku Drama', impressions: 4911, visits: 7970, visitRate: 0.1623 }
      ],
      zipCodes: [
        { zip: '60430', dma: 'CHICAGO', population: 45600, impressions: 5266, visits: 1579 },
        { zip: '10123', dma: 'NEW YORK', population: 45600, impressions: 4893, visits: 1586 },
        { zip: '85001', dma: 'PHOENIX (PRESCOTT)', population: 52100, impressions: 4250, visits: 2445 },
        { zip: '98111', dma: 'SEATTLE - TACOMA', population: 24800, impressions: 3998, visits: 388 },
        { zip: '30901', dma: 'AUGUSTA', population: 18500, impressions: 3541, visits: 673 },
        { zip: '28602', dma: 'CHARLOTTE', population: 35200, impressions: 3061, visits: 571 },
        { zip: '83707', dma: 'BOISE', population: 15600, impressions: 2976, visits: 779 },
        { zip: '30302', dma: 'ATLANTA', population: 12500, impressions: 2635, visits: 630 }
      ],
      dmaZipCounts: { 'CHICAGO': 186, 'NEW YORK': 331, 'PHOENIX (PRESCOTT)': 145, 'SEATTLE - TACOMA': 89, 'AUGUSTA': 42, 'CHARLOTTE': 78, 'BOISE': 35, 'ATLANTA': 134 }
    },

    // MNTN - Northern Tool (44.4M imps, 34K visits, 0.08%)
    '45346': {
      summary: { totalImpressions: 44421118, totalVisits: 33971, overallVisitRate: 0.00077, liftVsControl: 18.9 },
      campaigns: [
        { id: 'nt1', name: 'Northern Tool - Power Equipment', impressions: 18000000, visits: 14400, visitRate: 0.00080 },
        { id: 'nt2', name: 'Northern Tool - Generators', impressions: 14500000, visits: 10875, visitRate: 0.00075 },
        { id: 'nt3', name: 'Northern Tool - Retargeting', impressions: 11921118, visits: 8696, visitRate: 0.00073 }
      ],
      publishers: [
        { code: 'NBC', impressions: 31360, visits: 44082, visitRate: 0.1406 },
        { code: 'HBO Max', impressions: 12749, visits: 17467, visitRate: 0.1370 },
        { code: 'Peacock', impressions: 10135, visits: 14758, visitRate: 0.1456 },
        { code: 'Paramount Streaming - Comedy', impressions: 9447, visits: 13466, visitRate: 0.1426 },
        { code: 'Tubi Entertainment', impressions: 7765, visits: 11002, visitRate: 0.1417 },
        { code: 'Paramount Streaming - Drama', impressions: 7746, visits: 10576, visitRate: 0.1365 },
        { code: 'Paramount Streaming - Action-Thriller', impressions: 7101, visits: 9566, visitRate: 0.1347 },
        { code: 'Roku News', impressions: 5737, visits: 8218, visitRate: 0.1432 }
      ],
      zipCodes: [
        { zip: '55440', dma: 'MINNEAPOLIS - ST. PAUL', population: 8996, impressions: 7813, visits: 821 },
        { zip: '75270', dma: 'DALLAS - FT. WORTH', population: 15234, impressions: 7186, visits: 867 },
        { zip: '77052', dma: 'HOUSTON', population: 22456, impressions: 6461, visits: 910 },
        { zip: '10123', dma: 'NEW YORK', population: 45600, impressions: 6340, visits: 276 },
        { zip: '78427', dma: 'CORPUS CHRISTI', population: 8900, impressions: 5405, visits: 115 },
        { zip: '30302', dma: 'ATLANTA', population: 12500, impressions: 4355, visits: 337 },
        { zip: '56387', dma: 'MINNEAPOLIS - ST. PAUL', population: 6500, impressions: 3809, visits: 400 },
        { zip: '78288', dma: 'SAN ANTONIO', population: 18700, impressions: 3181, visits: 436 }
      ],
      dmaZipCounts: { 'MINNEAPOLIS - ST. PAUL': 145, 'DALLAS - FT. WORTH': 167, 'HOUSTON': 178, 'NEW YORK': 331, 'ATLANTA': 134, 'SAN ANTONIO': 98, 'CORPUS CHRISTI': 45 }
    },

    // MNTN - Visit PA (2.8M imps, 136K visits, 4.78% - highest rate!)
    '32623': {
      summary: { totalImpressions: 2847416, totalVisits: 136150, overallVisitRate: 0.04782, liftVsControl: 52.3 },
      campaigns: [
        { id: 'vp1', name: 'Visit PA - Winter Campaign', impressions: 1200000, visits: 60000, visitRate: 0.05000 },
        { id: 'vp2', name: 'Visit PA - Fall Foliage', impressions: 950000, visits: 42750, visitRate: 0.04500 },
        { id: 'vp3', name: 'Visit PA - Summer Adventures', impressions: 697416, visits: 33400, visitRate: 0.04789 }
      ],
      publishers: [
        { code: 'easybrain.com', impressions: 6193, visits: 58309, visitRate: 0.9415 },
        { code: 'Tubi Entertainment', impressions: 4106, visits: 42233, visitRate: 1.0286 },
        { code: 'dailymotion.com', impressions: 4640, visits: 41456, visitRate: 0.8934 },
        { code: 'NBC', impressions: 5632, visits: 40538, visitRate: 0.7198 },
        { code: 'msn.com', impressions: 4123, visits: 36637, visitRate: 0.8886 },
        { code: 'bluestacks.com', impressions: 2437, visits: 36045, visitRate: 1.4790 },
        { code: 'dailymail.co.uk', impressions: 3373, visits: 30739, visitRate: 0.9114 },
        { code: 'nypost.com', impressions: 3170, visits: 30086, visitRate: 0.9492 }
      ],
      zipCodes: [
        { zip: '20149', dma: 'WASHINGTON, DC (HAGRSTWN)', population: 64197, impressions: 169290, visits: 8543 },
        { zip: '10123', dma: 'NEW YORK', population: 45600, impressions: 130833, visits: 12890 },
        { zip: '08899', dma: 'NEW YORK', population: 28500, impressions: 120206, visits: 965 },
        { zip: '44118', dma: 'CLEVELAND - AKRON (CANTON)', population: 42000, impressions: 81046, visits: 1641 },
        { zip: '20068', dma: 'WASHINGTON, DC (HAGRSTWN)', population: 2207, impressions: 35929, visits: 3226 },
        { zip: '75270', dma: 'DALLAS - FT. WORTH', population: 15234, impressions: 33030, visits: 818 },
        { zip: '11427', dma: 'NEW YORK', population: 35600, impressions: 32337, visits: 3327 },
        { zip: '82801', dma: 'RAPID CITY', population: 12500, impressions: 28465, visits: 589 }
      ],
      dmaZipCounts: { 'WASHINGTON, DC (HAGRSTWN)': 294, 'NEW YORK': 331, 'CLEVELAND - AKRON (CANTON)': 98, 'DALLAS - FT. WORTH': 167, 'RAPID CITY': 25 }
    },

    // Causal iQ - Lowes TTD (PT=6 - SITE field for context)
    '45141': {
      summary: { totalImpressions: 6991269, totalVisits: 404283, overallVisitRate: 0.0578, liftVsControl: 32.1 },
      campaigns: [],
      contexts: [
        { code: 'mail.yahoo.com', impressions: 104784, visits: 369361, visitRate: 0.0352 },
        { code: 'mail.aol.com', impressions: 18046, visits: 55000, visitRate: 0.0305 },
        { code: '6468921495', impressions: 9065, visits: 36114, visitRate: 0.0398 },
        { code: 'solitaired.com', impressions: 8737, visits: 33247, visitRate: 0.0381 },
        { code: 'com.radio.pocketfm', impressions: 6788, visits: 35393, visitRate: 0.0521 },
        { code: 'dtapp.u.gg', impressions: 4255, visits: 14020, visitRate: 0.0329 },
        { code: 'www.solitairebliss.com', impressions: 2855, visits: 9831, visitRate: 0.0344 },
        { code: 'tunein.player', impressions: 2820, visits: 15522, visitRate: 0.0550 }
      ],
      zipCodes: [
        { zip: '28202', dma: 'CHARLOTTE', population: 45000, impressions: 380000, visits: 22800 },
        { zip: '27601', dma: 'RALEIGH-DURHAM', population: 38000, impressions: 320000, visits: 18560 },
        { zip: '30301', dma: 'ATLANTA', population: 55600, impressions: 450000, visits: 24750 },
        { zip: '75201', dma: 'DALLAS', population: 48700, impressions: 410000, visits: 21730 },
        { zip: '77001', dma: 'HOUSTON', population: 58200, impressions: 480000, visits: 26400 }
      ],
      dmaZipCounts: { 'CHARLOTTE': 89, 'RALEIGH-DURHAM': 76, 'ATLANTA': 134, 'DALLAS': 167, 'HOUSTON': 178 }
    },

    // Causal iQ - Golden Nugget Casino (PT=6 - SITE field for context)
    '27588': {
      summary: { totalImpressions: 11800000, totalVisits: 633152, overallVisitRate: 0.0537, liftVsControl: 28.5 },
      campaigns: [],
      contexts: [
        { code: 'mail.yahoo.com', impressions: 9275, visits: 36414, visitRate: 0.0393 },
        { code: 'www.yahoo.com', impressions: 5630, visits: 32124, visitRate: 0.0571 },
        { code: 'www.foxnews.com', impressions: 2100, visits: 8500, visitRate: 0.0405 },
        { code: 'solitaired.com', impressions: 1850, visits: 7200, visitRate: 0.0389 },
        { code: 'www.lotterypost.com', impressions: 1420, visits: 5800, visitRate: 0.0408 }
      ],
      zipCodes: [
        { zip: '89101', dma: 'LAS VEGAS', population: 42000, impressions: 850000, visits: 51000 },
        { zip: '10001', dma: 'NEW YORK', population: 82500, impressions: 1200000, visits: 60000 },
        { zip: '19101', dma: 'PHILADELPHIA', population: 54000, impressions: 780000, visits: 39000 },
        { zip: '70112', dma: 'NEW ORLEANS', population: 38000, impressions: 520000, visits: 28600 }
      ],
      dmaZipCounts: { 'LAS VEGAS': 45, 'NEW YORK': 331, 'PHILADELPHIA': 112, 'NEW ORLEANS': 67 }
    },

    // Causal iQ - Space Coast Tourism (NO PUBLISHER DATA - GEO ONLY)
    '7389': {
      summary: { totalImpressions: 147142878, totalVisits: 5702339, overallVisitRate: 0.03875, liftVsControl: 45.6 },
      campaigns: [], // No campaign names available
      publishers: [], // No publisher attribution available
      zipCodes: [
        { zip: '10123', dma: 'NEW YORK', population: 0, impressions: 2831300, visits: 115624 },
        { zip: '30302', dma: 'ATLANTA', population: 28077, impressions: 1267043, visits: 97007 },
        { zip: '33102', dma: 'MIAMI - FT. LAUDERDALE', population: 0, impressions: 1229889, visits: 187226 },
        { zip: '32811', dma: 'ORLANDO - DAYTONA BCH', population: 41246, impressions: 1168239, visits: 534750 },
        { zip: '32802', dma: 'ORLANDO - DAYTONA BCH', population: 12174, impressions: 1145481, visits: 245380 },
        { zip: '20149', dma: 'WASHINGTON, DC', population: 48110, impressions: 1060622, visits: 36507 },
        { zip: '60666', dma: 'CHICAGO', population: 9926, impressions: 1047066, visits: 29447 },
        { zip: '20068', dma: 'WASHINGTON, DC', population: 2207, impressions: 885056, visits: 13407 },
        { zip: '19099', dma: 'PHILADELPHIA', population: 13603, impressions: 825804, visits: 16361 },
        { zip: '33606', dma: 'TAMPA - ST. PETE', population: 19466, impressions: 630796, visits: 78338 },
        { zip: '34758', dma: 'ORLANDO - DAYTONA BCH', population: 41094, impressions: 447993, visits: 195119 },
        { zip: '32920', dma: 'ORLANDO - DAYTONA BCH', population: 10181, impressions: 422592, visits: 178407 }
      ],
      dmaZipCounts: { 'ORLANDO - DAYTONA BCH': 145, 'MIAMI - FT. LAUDERDALE': 89, 'NEW YORK': 67, 'TAMPA - ST. PETE': 54, 'ATLANTA': 48 },
      noPublisherData: true
    },

    // Causal iQ - AutoZone (PT=6 - SITE field for context)
    '8123': {
      summary: { totalImpressions: 25371271, totalVisits: 869868, overallVisitRate: 0.03429, liftVsControl: 38.4 },
      campaigns: [],
      contexts: [
        { code: 'poki.com', impressions: 1723, visits: 6313, visitRate: 0.0366 },
        { code: 'www.crazygames.com', impressions: 1450, visits: 3278, visitRate: 0.0226 },
        { code: 'spektra.games', impressions: 1052, visits: 9350, visitRate: 0.0889 },
        { code: 'spacesurvival.online', impressions: 1184, visits: 5355, visitRate: 0.0452 },
        { code: 'www.thingiverse.com', impressions: 1044, visits: 2313, visitRate: 0.0222 },
        { code: 'sirstudios.com', impressions: 760, visits: 91561, visitRate: 1.2047 },
        { code: 'lionstudios.cc', impressions: 740, visits: 3528, visitRate: 0.0477 },
        { code: 'turborilla.com', impressions: 643, visits: 4271, visitRate: 0.0664 }
      ],
      zipCodes: [
        { zip: '10123', dma: 'NEW YORK', population: 0, impressions: 310823, visits: 15704 },
        { zip: '60666', dma: 'CHICAGO', population: 9926, impressions: 198426, visits: 9266 },
        { zip: '85001', dma: 'PHOENIX', population: 44224, impressions: 187971, visits: 15220 },
        { zip: '20149', dma: 'WASHINGTON, DC', population: 48110, impressions: 135424, visits: 5945 },
        { zip: '30302', dma: 'ATLANTA', population: 28077, impressions: 118896, visits: 11226 },
        { zip: '75270', dma: 'DALLAS - FT. WORTH', population: 0, impressions: 111404, visits: 8716 },
        { zip: '20163', dma: 'WASHINGTON, DC', population: 4448, impressions: 102776, visits: 5749 },
        { zip: '20068', dma: 'WASHINGTON, DC', population: 2207, impressions: 99623, visits: 10260 },
        { zip: '14580', dma: 'ROCHESTER, NY', population: 52587, impressions: 97761, visits: 44652 },
        { zip: '19099', dma: 'PHILADELPHIA', population: 13603, impressions: 95964, visits: 4778 },
        { zip: '90009', dma: 'LOS ANGELES', population: 31739, impressions: 68605, visits: 5274 },
        { zip: '64121', dma: 'KANSAS CITY', population: 383, impressions: 60573, visits: 3413 }
      ],
      dmaZipCounts: { 'NEW YORK': 112, 'CHICAGO': 98, 'WASHINGTON, DC': 87, 'PHOENIX': 65, 'ATLANTA': 58, 'DALLAS - FT. WORTH': 52 }
    },

    // ViacomCBS - Lean RX (Web Visits)
    '40514': {
      summary: { totalImpressions: 1432999, totalVisits: 23896, overallVisitRate: 0.01667, liftVsControl: 23.4 },
      campaigns: [],
      contexts: [
        { code: 'g1080220', impressions: 321745, visits: 4790, visitRate: 0.0149 },
        { code: 'g1080212', impressions: 100890, visits: 4421, visitRate: 0.0438 },
        { code: 'g1094458', impressions: 86721, visits: 260, visitRate: 0.0030 },
        { code: 'g1080222', impressions: 79873, visits: 859, visitRate: 0.0108 },
        { code: 'g965628', impressions: 72559, visits: 1308, visitRate: 0.0180 },
        { code: 'g1080218', impressions: 66201, visits: 173, visitRate: 0.0026 },
        { code: 'g1080210', impressions: 59723, visits: 1111, visitRate: 0.0186 },
        { code: 'g1080211', impressions: 31280, visits: 1474, visitRate: 0.0471 }
      ],
      postalCodes: [
        { zip: '30334', impressions: 3981, visits: 159, visitRate: 0.0399 },
        { zip: '60602', impressions: 3742, visits: 233, visitRate: 0.0623 },
        { zip: '75202', impressions: 3217, visits: 184, visitRate: 0.0572 },
        { zip: '98101', impressions: 2395, visits: 165, visitRate: 0.0689 },
        { zip: '20147', impressions: 2050, visits: 89, visitRate: 0.0434 }
      ],
      trafficSources: [
        { source: 'Telly', impressions: 2229, visits: 89, visitRate: 0.0399 },
        { source: 'Paramount+', impressions: 7960, visits: 311, visitRate: 0.0391 },
        { source: 'Google/DV360', impressions: 46253, visits: 1713, visitRate: 0.0370 },
        { source: 'Amazon', impressions: 16608, visits: 609, visitRate: 0.0367 },
        { source: 'Pluto TV', impressions: 53121, visits: 1860, visitRate: 0.0350 }
      ]
    }
  };
  
  useEffect(() => {
    setAgencies(realAgencies);
  }, []);
  
  // ============================================================
  // STANDARD CALCULATIONS
  // ============================================================
  const calcPriority = (item, baseline, totalImpressions) => {
    const impShare = item.impressions / totalImpressions;
    const perfRatio = baseline > 0 ? item.visitRate / baseline : 0;
    return Math.round((impShare * 1000) - (perfRatio * 100));
  };

  const calcIndex = (itemRate, baselineRate) => {
    return baselineRate > 0 ? Math.round((itemRate / baselineRate) * 100) : 0;
  };

  const getPriorityColor = (priority) => {
    if (priority > 100) return '#ff4444';
    if (priority > 50) return '#ff8844';
    if (priority > 0) return '#ffbb44';
    if (priority > -50) return '#88dd88';
    return '#44cc44';
  };

  const getIndexColor = (index) => {
    if (index >= 120) return '#3ddc97';
    if (index >= 100) return '#88dd88';
    if (index >= 70) return '#e7eefc';
    return '#ff6b6b';
  };
  
  // ============================================================
  // ANALYZE DATA
  // ============================================================
  const analyzeData = () => {
    if (!selectedAdvertiser) return;
    setLoading(true);
    
    setTimeout(() => {
      const advData = advertiserData[selectedAdvertiser];
      if (!advData) {
        setLoading(false);
        return;
      }
      
      const isWebVisits = selectedAgency === '1480';
      const baseline = advData.summary.overallVisitRate;
      const totalImps = advData.summary.totalImpressions;
      
      // Enrich campaigns
      const campaigns = (advData.campaigns || []).map(c => ({
        ...c,
        index: calcIndex(c.visitRate, baseline)
      }));
      
      // Enrich publishers/contexts
      const publishers = (advData.publishers || advData.contexts || []).map(p => ({
        ...p,
        reallocationPriority: calcPriority(p, baseline, totalImps)
      }));
      
      // Enrich ZIPs with priority
      const totalPop = (advData.zipCodes || []).reduce((s, z) => s + (z.population || 0), 0);
      const zipCodes = (advData.zipCodes || []).map(z => {
        const visitRate = z.impressions > 0 ? (z.visits / z.impressions) : (z.visitRate || 0);
        const zipImpShare = z.impressions / totalImps;
        const zipPopShare = totalPop > 0 ? (z.population || 0) / totalPop : 0;
        const popWeightedIdx = zipPopShare > 0 ? Math.round((zipImpShare / zipPopShare) * 100) : 0;
        const perfIdx = calcIndex(visitRate, baseline);
        return {
          ...z,
          visitRate,
          reallocationPriority: popWeightedIdx > 0 ? popWeightedIdx - perfIdx : calcPriority({ impressions: z.impressions, visitRate }, baseline, totalImps)
        };
      });
      
      // Calculate recommendations for publishers
      const sortedPubs = [...publishers].sort((a, b) => b.reallocationPriority - a.reallocationPriority);
      const consPubs = sortedPubs.filter(p => p.reallocationPriority > 0 && p.visits > 0).slice(0, 5);
      const aggPubs = sortedPubs.filter(p => p.reallocationPriority > -20).slice(0, 8);
      
      // Calculate recommendations for ZIPs
      const sortedZips = [...zipCodes].sort((a, b) => b.reallocationPriority - a.reallocationPriority);
      const dmaZipCounts = advData.dmaZipCounts || {};
      const consZips = sortedZips.filter(z => z.reallocationPriority > 0 && z.visits > 0 && (!z.dma || (dmaZipCounts[z.dma] || 999) > 5)).slice(0, 5);
      const aggZips = sortedZips.filter(z => z.reallocationPriority > -50 && (!z.dma || (dmaZipCounts[z.dma] || 999) > 5)).slice(0, 10);
      
      const calcProj = (items) => {
        const excImps = items.reduce((s, i) => s + i.impressions, 0);
        const excVisits = items.reduce((s, i) => s + i.visits, 0);
        const newRate = (advData.summary.totalVisits - excVisits) / (totalImps - excImps);
        return { newRate, improvement: ((newRate / baseline) - 1) * 100, excImps };
      };
      
      setData({
        type: isWebVisits ? 'web_visits' : 'store_visits',
        summary: advData.summary,
        campaigns,
        publishers: isWebVisits ? [] : publishers,
        contexts: isWebVisits ? publishers : [],
        zipCodes,
        trafficSources: advData.trafficSources || [],
        noPublisherData: advData.noPublisherData || false,
        pubRecommendations: publishers.length > 0 ? {
          conservative: { ...calcProj(consPubs), count: consPubs.length, items: consPubs },
          aggressive: { ...calcProj(aggPubs), count: aggPubs.length, items: aggPubs }
        } : null,
        geoRecommendations: {
          conservative: { ...calcProj(consZips), count: consZips.length, items: consZips },
          aggressive: { ...calcProj(aggZips), count: aggZips.length, items: aggZips }
        }
      });
      setLoading(false);
    }, 600);
  };
  
  const handleAgencyClick = (agencyId) => {
    if (expandedAgency === agencyId) {
      setExpandedAgency('');
    } else {
      setExpandedAgency(agencyId);
      setSelectedAgency(agencyId);
      setSelectedAdvertiser('');
      setData(null);
      setAdvertisers(realAdvertisers[agencyId] || []);
    }
  };
  
  const exportToCSV = (items, filename, columns) => {
    if (!items || items.length === 0) return;
    const headers = columns.map(c => c.label);
    const rows = items.map(item => columns.map(c => {
      const val = item[c.key];
      return c.format ? c.format(val).replace('%', '') : val;
    }));
    const csv = [headers, ...rows].map(row => row.join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${filename}.csv`;
    a.click();
  };
  
  const copyToClipboard = (items, key, id) => {
    if (!items) return;
    const text = items.map(item => item[key]).join(', ');
    navigator.clipboard.writeText(text);
    setCopied(id);
    setTimeout(() => setCopied(''), 2000);
  };
  
  // ============================================================
  // UI COMPONENTS
  // ============================================================
  const MethodologyTooltip = () => (
    <div style={{ position: 'relative', display: 'inline-block' }}>
      <span onMouseEnter={() => setShowMethodology(true)} onMouseLeave={() => setShowMethodology(false)}
        style={{ cursor: 'help', marginLeft: '8px', background: 'rgba(122,162,255,.3)', borderRadius: '50%', width: '18px', height: '18px', display: 'inline-flex', alignItems: 'center', justifyContent: 'center', fontSize: '12px', fontWeight: 'bold' }}>?</span>
      {showMethodology && (
        <div style={{ position: 'absolute', top: '24px', left: '-150px', width: '320px', background: 'rgba(20,30,50,.98)', border: '1px solid rgba(122,162,255,.3)', borderRadius: '8px', padding: '16px', zIndex: 1000, fontSize: '11px', lineHeight: '1.6', boxShadow: '0 8px 24px rgba(0,0,0,.4)' }}>
          <div style={{ fontWeight: 'bold', marginBottom: '8px', color: '#7aa2ff' }}>Lift vs Control Methodology</div>
          <div style={{ color: '#a8b6d8' }}>
            <p><strong>Exposed Group:</strong> Users who saw ad impressions and subsequently visited</p>
            <p><strong>Control Group:</strong> Baseline visit rate from non-exposed population in same DMAs</p>
            <p><strong>Lift:</strong><br/><code style={{ background: 'rgba(0,0,0,.3)', padding: '2px 6px', borderRadius: '4px' }}>((Exposed - Control) / Control) × 100</code></p>
          </div>
        </div>
      )}
    </div>
  );

  const KPICards = ({ summary, type }) => (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '20px', marginBottom: '24px' }}>
      {[
        { label: 'Visit Rate', value: (summary.overallVisitRate * 100).toFixed(2) + '%', color: '#7aa2ff' },
        { label: 'Lift vs Control', value: '+' + summary.liftVsControl.toFixed(1) + '%', color: '#3ddc97', hasTooltip: true },
        { label: 'Total Impressions', value: summary.totalImpressions >= 1000000 ? (summary.totalImpressions / 1000000).toFixed(1) + 'M' : (summary.totalImpressions / 1000).toFixed(0) + 'K', color: '#a8b6d8' },
        { label: type === 'web_visits' ? 'Web Visits' : 'Store Visits', value: summary.totalVisits >= 1000000 ? (summary.totalVisits / 1000000).toFixed(2) + 'M' : summary.totalVisits.toLocaleString(), color: '#3ddc97' }
      ].map((kpi, i) => (
        <div key={i} style={{ background: 'linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.03))', border: '1px solid rgba(255,255,255,.1)', borderRadius: '12px', padding: '20px' }}>
          <div style={{ fontSize: '11px', color: '#a8b6d8', marginBottom: '8px', fontWeight: 600, textTransform: 'uppercase', display: 'flex', alignItems: 'center' }}>
            {kpi.label}{kpi.hasTooltip && <MethodologyTooltip />}
          </div>
          <div style={{ fontSize: '28px', fontWeight: 700, fontFamily: 'monospace', color: kpi.color }}>{kpi.value}</div>
        </div>
      ))}
    </div>
  );

  const ReallocationCards = ({ recommendations, tier }) => {
    const rec = recommendations[tier];
    return (
      <div style={{ background: 'rgba(0,0,0,.2)', border: '1px solid rgba(255,255,255,.08)', borderRadius: '8px', padding: '16px' }}>
        <div style={{ fontSize: '11px', color: '#a8b6d8', marginBottom: '8px', fontWeight: 600, textTransform: 'uppercase' }}>{tier} Reallocation</div>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: '12px', marginBottom: '8px' }}>
          <div style={{ fontSize: '24px', fontWeight: 700, color: '#3ddc97' }}>+{rec.improvement.toFixed(1)}%</div>
          <div style={{ fontSize: '14px', color: '#7aa2ff' }}>→ {(rec.newRate * 100).toFixed(3)}%</div>
        </div>
        <div style={{ fontSize: '11px', color: '#a8b6d8' }}>Reallocate {rec.excImps.toLocaleString()} imps from {rec.count} items</div>
      </div>
    );
  };

  const DataTable = ({ title, subtitle, data, columns, recommendations, copyKey, exportName, emptyMessage }) => {
    if ((!data || data.length === 0) && emptyMessage) {
      return (
        <div style={{ background: 'linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.02))', border: '1px solid rgba(255,255,255,.08)', borderRadius: '18px', padding: '24px', marginBottom: '24px' }}>
          <h3 style={{ margin: '0 0 8px', fontSize: '16px', color: '#606875' }}>{title}</h3>
          <p style={{ margin: 0, color: '#606875', fontSize: '12px' }}>{emptyMessage}</p>
        </div>
      );
    }
    
    if (!data || data.length === 0) return null;
    
    return (
      <div style={{ background: 'linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.03))', border: '1px solid rgba(255,255,255,.1)', borderRadius: '18px', padding: '24px', marginBottom: '24px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
          <div>
            <h3 style={{ margin: 0, fontSize: '16px' }}>{title}</h3>
            {subtitle && <span style={{ fontSize: '11px', color: '#a8b6d8' }}>{subtitle}</span>}
          </div>
          <div style={{ display: 'flex', gap: '8px' }}>
            <button onClick={() => exportToCSV(data, exportName, columns)} style={{ padding: '6px 12px', background: 'rgba(122,162,255,.2)', border: '1px solid rgba(122,162,255,.3)', borderRadius: '4px', color: '#7aa2ff', fontSize: '11px', cursor: 'pointer' }}>📥 CSV</button>
            <button onClick={() => copyToClipboard(data, copyKey, exportName)} style={{ padding: '6px 12px', background: copied === exportName ? 'rgba(61,220,151,.2)' : 'rgba(122,162,255,.2)', border: `1px solid ${copied === exportName ? 'rgba(61,220,151,.3)' : 'rgba(122,162,255,.3)'}`, borderRadius: '4px', color: copied === exportName ? '#3ddc97' : '#7aa2ff', fontSize: '11px', cursor: 'pointer' }}>{copied === exportName ? '✓ Copied' : '📋 Copy'}</button>
          </div>
        </div>
        
        {recommendations && (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '16px', marginBottom: '20px' }}>
            <ReallocationCards recommendations={recommendations} tier="conservative" />
            <ReallocationCards recommendations={recommendations} tier="aggressive" />
          </div>
        )}
        
        <div style={{ maxHeight: '350px', overflowY: 'auto', background: 'rgba(0,0,0,.2)', borderRadius: '8px', padding: '12px' }}>
          <table style={{ width: '100%', fontSize: '11px', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid rgba(255,255,255,.1)', position: 'sticky', top: 0, background: 'rgba(0,0,0,.7)' }}>
                {columns.map(col => (
                  <th key={col.key} style={{ textAlign: col.align || 'left', padding: '8px', color: '#a8b6d8' }}>{col.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.sort((a, b) => (b.reallocationPriority || b.index || 0) - (a.reallocationPriority || a.index || 0)).map((row, i) => (
                <tr key={i} style={{ borderBottom: '1px solid rgba(255,255,255,.05)' }}>
                  {columns.map(col => {
                    const val = row[col.key];
                    const isPriority = col.key === 'reallocationPriority';
                    const isIndex = col.key === 'index';
                    return (
                      <td key={col.key} style={{ 
                        padding: '8px', 
                        textAlign: col.align || 'left', 
                        fontFamily: col.mono ? 'monospace' : 'inherit',
                        fontWeight: col.bold || isPriority || isIndex ? 600 : 'normal',
                        color: isPriority ? getPriorityColor(val) : isIndex ? getIndexColor(val) : (col.color || '#e7eefc'),
                        background: isPriority ? getPriorityColor(val) + '30' : 'transparent',
                        maxWidth: col.maxWidth || 'auto',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap'
                      }}>
                        {col.format ? col.format(val) : val}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };
  
  const tabs = [
    { id: 'location-visits', label: 'Location Visits', enabled: true },
    { id: 'web-events', label: 'Web Events', enabled: false },
    { id: 'audience-explorer', label: 'Audience Explorer', enabled: false }
  ];
  
  const getAdvertiserInitials = () => {
    const adv = advertisers.find(([id]) => id === selectedAdvertiser);
    if (!adv) return '?';
    return adv[1].split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase();
  };
  
  // ============================================================
  // RENDER
  // ============================================================
  return (
    <div style={{ minHeight: '100vh', background: 'radial-gradient(1200px 800px at 20% -10%, rgba(122,162,255,.22), transparent 60%), radial-gradient(900px 700px at 110% 10%, rgba(61,220,151,.15), transparent 55%), #0b1220', color: '#e7eefc', display: 'flex', fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif', userSelect: isResizing ? 'none' : 'auto' }}>
      {/* Sidebar */}
      <div style={{ 
        width: sidebarCollapsed ? collapsedWidth : sidebarWidth, 
        minWidth: sidebarCollapsed ? collapsedWidth : sidebarWidth,
        background: 'linear-gradient(180deg, rgba(11,18,32,.98), rgba(15,23,38,.98))', 
        borderRight: '1px solid rgba(255,255,255,.08)', 
        display: 'flex', 
        flexDirection: 'column', 
        maxHeight: '100vh',
        transition: sidebarCollapsed ? 'width 0.2s, min-width 0.2s' : 'none',
        position: 'relative'
      }}>
        <button onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
          style={{ position: 'absolute', top: '50%', right: '-12px', transform: 'translateY(-50%)', width: '24px', height: '48px', background: 'linear-gradient(180deg, rgba(30,40,60,.98), rgba(20,30,50,.98))', border: '1px solid rgba(255,255,255,.15)', borderRadius: '0 6px 6px 0', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#a8b6d8', fontSize: '12px', zIndex: 100 }}>
          {sidebarCollapsed ? '▶' : '◀'}
        </button>

        {!sidebarCollapsed && (
          <div onMouseDown={handleMouseDown}
            style={{ position: 'absolute', top: 0, right: 0, width: '4px', height: '100%', cursor: 'col-resize', background: isResizing ? 'rgba(122,162,255,.4)' : 'transparent', zIndex: 50 }} />
        )}

        <div style={{ padding: sidebarCollapsed ? '24px 8px' : '24px 20px', borderBottom: '1px solid rgba(255,255,255,.08)', textAlign: sidebarCollapsed ? 'center' : 'left' }}>
          {sidebarCollapsed ? (
            <div style={{ fontSize: '18px', fontWeight: 700, color: '#7aa2ff' }}>Q</div>
          ) : (
            <>
              <h1 style={{ margin: 0, fontSize: '20px', fontWeight: 700, background: 'linear-gradient(135deg, #fff 0%, #a8b6d8 100%)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>Quorum Optimizer</h1>
              <p style={{ margin: '4px 0 0', color: '#606875', fontSize: '12px' }}>Campaign Analytics</p>
            </>
          )}
        </div>

        {!sidebarCollapsed && (
          <>
            <div style={{ padding: '16px 20px', borderBottom: '1px solid rgba(255,255,255,.08)' }}>
              <input type="text" placeholder="Search agencies..." value={agencySearch} onChange={(e) => setAgencySearch(e.target.value)}
                style={{ width: '100%', padding: '8px 12px', background: 'rgba(255,255,255,.05)', border: '1px solid rgba(255,255,255,.12)', borderRadius: '6px', color: '#e7eefc', fontSize: '13px' }} />
            </div>

            <div style={{ flex: 1, overflowY: 'auto', padding: '8px 0' }}>
              {agencies.filter(([id, name]) => !agencySearch || name.toLowerCase().includes(agencySearch.toLowerCase())).map(([agencyId, agencyName, type]) => (
                <div key={agencyId}>
                  <div onClick={() => handleAgencyClick(agencyId)}
                    style={{ padding: '10px 20px', cursor: 'pointer', background: selectedAgency === agencyId ? 'rgba(122,162,255,.12)' : 'transparent', borderLeft: selectedAgency === agencyId ? '3px solid #7aa2ff' : '3px solid transparent', fontSize: '13px', fontWeight: 600, color: selectedAgency === agencyId ? '#e7eefc' : '#a8b6d8', display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{ transform: expandedAgency === agencyId ? 'rotate(90deg)' : 'rotate(0deg)', fontSize: '10px', transition: 'transform 0.2s' }}>▶</span>
                    {agencyName}
                    <span style={{ marginLeft: 'auto', fontSize: '10px', padding: '2px 6px', background: type === 'Web' ? 'rgba(122,162,255,.2)' : 'rgba(61,220,151,.2)', borderRadius: '4px', color: type === 'Web' ? '#7aa2ff' : '#3ddc97' }}>
                      {type}
                    </span>
                  </div>
                  {expandedAgency === agencyId && advertisers.map(([advId, advName]) => (
                    <div key={advId} onClick={() => { setSelectedAdvertiser(advId); setData(null); }}
                      style={{ padding: '8px 20px 8px 48px', cursor: 'pointer', background: selectedAdvertiser === advId ? 'rgba(122,162,255,.12)' : 'rgba(0,0,0,.15)', fontSize: '12px', color: selectedAdvertiser === advId ? '#e7eefc' : '#a8b6d8', borderLeft: selectedAdvertiser === advId ? '3px solid #7aa2ff' : '3px solid transparent' }}>
                      {advName}
                    </div>
                  ))}
                </div>
              ))}
            </div>

            <div style={{ padding: '16px 20px', borderTop: '1px solid rgba(255,255,255,.08)', background: 'rgba(0,0,0,.2)' }}>
              <label style={{ display: 'block', fontSize: '11px', color: '#a8b6d8', marginBottom: '8px', fontWeight: 600, textTransform: 'uppercase' }}>Date Range</label>
              <input type="date" value={dateRange.start} onChange={(e) => setDateRange({...dateRange, start: e.target.value})} style={{ width: '100%', padding: '8px 10px', marginBottom: '8px', background: 'rgba(255,255,255,.05)', border: '1px solid rgba(255,255,255,.12)', borderRadius: '6px', color: '#e7eefc', fontSize: '12px' }} />
              <input type="date" value={dateRange.end} onChange={(e) => setDateRange({...dateRange, end: e.target.value})} style={{ width: '100%', padding: '8px 10px', marginBottom: '12px', background: 'rgba(255,255,255,.05)', border: '1px solid rgba(255,255,255,.12)', borderRadius: '6px', color: '#e7eefc', fontSize: '12px' }} />
              <button onClick={analyzeData} disabled={!selectedAdvertiser || loading}
                style={{ width: '100%', padding: '10px', background: selectedAdvertiser && !loading ? 'linear-gradient(135deg, #7aa2ff, #5a82df)' : 'rgba(122,162,255,.3)', border: 'none', borderRadius: '6px', color: '#fff', fontSize: '13px', fontWeight: 600, cursor: selectedAdvertiser && !loading ? 'pointer' : 'not-allowed' }}>
                {loading ? 'Analyzing...' : 'Analyze Campaign'}
              </button>
            </div>
          </>
        )}

        {sidebarCollapsed && selectedAdvertiser && (
          <div style={{ padding: '16px 8px', textAlign: 'center' }}>
            <div style={{ width: '32px', height: '32px', margin: '0 auto', background: 'rgba(122,162,255,.2)', borderRadius: '6px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#7aa2ff', fontSize: '11px', fontWeight: 600 }}>
              {getAdvertiserInitials()}
            </div>
          </div>
        )}
      </div>

      {/* Main Content */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', height: '100vh' }}>
        <div style={{ background: 'rgba(11,18,32,.6)', borderBottom: '1px solid rgba(255,255,255,.08)', padding: '0 32px', display: 'flex', gap: '24px' }}>
          {tabs.map(tab => (
            <button key={tab.id} onClick={() => tab.enabled && setActiveTab(tab.id)}
              style={{ padding: '16px 0', background: 'none', border: 'none', borderBottom: activeTab === tab.id ? '2px solid #7aa2ff' : '2px solid transparent', color: tab.enabled ? (activeTab === tab.id ? '#e7eefc' : '#a8b6d8') : '#606875', fontSize: '13px', fontWeight: 600, cursor: tab.enabled ? 'pointer' : 'not-allowed' }}>
              {tab.label}
            </button>
          ))}
        </div>

        <div style={{ flex: 1, overflowY: 'auto', padding: '32px' }}>
          {!data ? (
            <div style={{ background: 'linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.03))', border: '1px solid rgba(255,255,255,.1)', borderRadius: '18px', padding: '64px', textAlign: 'center' }}>
              <h2 style={{ margin: '0 0 12px', fontSize: '24px', fontWeight: 700 }}>{selectedAdvertiser ? 'Ready to Analyze' : 'Select Agency & Advertiser'}</h2>
              <p style={{ margin: 0, color: '#a8b6d8', fontSize: '14px' }}>
                {selectedAdvertiser ? 'Click "Analyze Campaign" to view optimization insights' : 'Choose from the sidebar to get started'}
              </p>
            </div>
          ) : (
            <>
              {/* 1. MOD-LIFT: KPI Summary */}
              <KPICards summary={data.summary} type={data.type} />

              {/* 2. MOD-CAMP: Campaign Performance */}
              <DataTable
                title="Campaign Performance"
                subtitle="MOD-CAMP • Performance by campaign"
                data={data.campaigns}
                columns={[
                  { key: 'name', label: 'Campaign', maxWidth: '280px' },
                  { key: 'impressions', label: 'Impressions', align: 'right', mono: true, format: v => v.toLocaleString() },
                  { key: 'visits', label: 'Visits', align: 'right', mono: true, color: '#3ddc97', format: v => v.toLocaleString() },
                  { key: 'visitRate', label: 'Visit Rate', align: 'right', mono: true, color: '#7aa2ff', format: v => (v * 100).toFixed(2) + '%' },
                  { key: 'index', label: 'Index', align: 'right', mono: true }
                ]}
                copyKey="name"
                exportName="campaign-performance"
                emptyMessage="No campaign-level data available for this advertiser"
              />

              {/* 3. MOD-CTX: Publisher/Context Optimization */}
              {data.type === 'store_visits' && (
                <DataTable
                  title="Publisher Optimization"
                  subtitle="MOD-CTX • CTV network/app performance"
                  data={data.publishers}
                  columns={[
                    { key: 'code', label: 'Publisher', bold: true, maxWidth: '220px' },
                    { key: 'impressions', label: 'Impressions', align: 'right', mono: true, format: v => v.toLocaleString() },
                    { key: 'visits', label: 'Visits', align: 'right', mono: true, color: '#3ddc97', format: v => v.toLocaleString() },
                    { key: 'visitRate', label: 'Visit Rate', align: 'right', mono: true, color: '#7aa2ff', format: v => (v * 100).toFixed(3) + '%' },
                    { key: 'reallocationPriority', label: 'Priority', align: 'right', mono: true }
                  ]}
                  recommendations={data.pubRecommendations}
                  copyKey="code"
                  exportName="publisher-optimization"
                  emptyMessage={data.noPublisherData ? "Publisher attribution not available for this advertiser (Causal iQ data limitation)" : null}
                />
              )}

              {data.type === 'web_visits' && data.contexts && data.contexts.length > 0 && (
                <DataTable
                  title="Context Optimization"
                  subtitle="MOD-CTX • FreeWheel show/content IDs"
                  data={data.contexts}
                  columns={[
                    { key: 'code', label: 'Context Code', mono: true, bold: true, color: '#7aa2ff' },
                    { key: 'impressions', label: 'Impressions', align: 'right', mono: true, format: v => v.toLocaleString() },
                    { key: 'visits', label: 'Web Visits', align: 'right', mono: true, color: '#3ddc97', format: v => v.toLocaleString() },
                    { key: 'visitRate', label: 'Visit Rate', align: 'right', mono: true, color: '#7aa2ff', format: v => (v * 100).toFixed(2) + '%' },
                    { key: 'reallocationPriority', label: 'Priority', align: 'right', mono: true }
                  ]}
                  recommendations={data.pubRecommendations}
                  copyKey="code"
                  exportName="context-optimization"
                />
              )}

              {/* 4. MOD-GEO: Geographic Optimization */}
              <DataTable
                title="Geographic Optimization"
                subtitle={`MOD-GEO • ${data.type === 'web_visits' ? 'Viewer home ZIP' : 'ZIP code performance'}`}
                data={data.zipCodes}
                columns={[
                  { key: 'dma', label: 'DMA', color: '#a8b6d8', maxWidth: '140px' },
                  { key: 'zip', label: 'ZIP', mono: true, bold: true, color: '#7aa2ff' },
                  { key: 'impressions', label: 'Impressions', align: 'right', mono: true, format: v => v.toLocaleString() },
                  { key: 'visits', label: 'Visits', align: 'right', mono: true, color: '#3ddc97', format: v => v.toLocaleString() },
                  { key: 'visitRate', label: 'Visit Rate', align: 'right', mono: true, color: '#7aa2ff', format: v => (v * 100).toFixed(3) + '%' },
                  { key: 'reallocationPriority', label: 'Priority', align: 'right', mono: true }
                ]}
                recommendations={data.geoRecommendations}
                copyKey="zip"
                exportName="geo-optimization"
              />

              {/* 5. MOD-TRF: Traffic Source (Web Visits only) */}
              {data.type === 'web_visits' && data.trafficSources && data.trafficSources.length > 0 && (
                <DataTable
                  title="Traffic Source Optimization"
                  subtitle="MOD-TRF • Ad platform performance (requires web pixel)"
                  data={data.trafficSources.map(t => ({ ...t, reallocationPriority: calcPriority(t, data.summary.overallVisitRate, data.summary.totalImpressions) }))}
                  columns={[
                    { key: 'source', label: 'Traffic Source', bold: true },
                    { key: 'impressions', label: 'Impressions', align: 'right', mono: true, format: v => v.toLocaleString() },
                    { key: 'visits', label: 'Web Visits', align: 'right', mono: true, color: '#3ddc97', format: v => v.toLocaleString() },
                    { key: 'visitRate', label: 'Visit Rate', align: 'right', mono: true, color: '#7aa2ff', format: v => (v * 100).toFixed(2) + '%' },
                    { key: 'reallocationPriority', label: 'Priority', align: 'right', mono: true }
                  ]}
                  copyKey="source"
                  exportName="traffic-source-optimization"
                />
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default QuorumOptimizer;
