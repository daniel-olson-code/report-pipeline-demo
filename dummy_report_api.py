"""
This module implements a FastAPI application for generating and managing dummy reports.

The application allows users to create reports, check their status, and download them
when ready. It uses SQLite for data persistence.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from uuid import uuid4
from typing import List
from datetime import datetime, timedelta
import random
import sqlite3

app = FastAPI()

# Define report types
REPORT_TYPES = ['Ad Campaigns', 'Ad Groups', 'Product Ads', 'Keywords', 'Product Targets']

# Connect to the SQLite database
conn = sqlite3.connect('dummy_report_db.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Create a table for reports if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS reports (
    id TEXT PRIMARY KEY,
    region TEXT,
    report_type TEXT,
    start_date TEXT,
    end_date TEXT,
    status TEXT,
    ready_time TEXT,
    data TEXT
)
''')
conn.commit()


class ReportRequest(BaseModel):
    """
    Represents a report request.

    Attributes:
        region (str): The region for which the report is requested.
        report_type (str): The type of report requested.
        start_date (datetime): The start date for the report data.
        end_date (datetime): The end date for the report data.
    """
    region: str
    report_type: str
    start_date: datetime
    end_date: datetime


class ReportData(BaseModel):
    """
    Represents a single entry in a report.

    Attributes:
        region (str): Region for which the data is relevant.
        id (str): Unique identifier for the entry.
        name (str): Name of the item.
        entity (str): Type of entity (e.g., Campaign, Ad Group).
        date (str): Date for which the data is relevant.
        sales (float): Total sales amount.
        spend (float): Total spend amount.
        clicks (int): Number of clicks.
        impressions (int): Number of impressions.
        orders (int): Number of orders.
        acos (float): Advertising Cost of Sales.
        roas (float): Return on Ad Spend.
        cvr (float): Conversion Rate.
        ctr (float): Click-Through Rate.
    """
    region: str
    id: str
    name: str
    entity: str
    date: str
    sales: float
    spend: float
    clicks: int
    impressions: int
    orders: int
    acos: float
    roas: float
    cvr: float
    ctr: float


def generate_report_data(region: str, report_type: str, start_date: datetime, end_date: datetime) -> List[ReportData]:
    """
    Generates random report data for the specified report type and date range.

    Args:
        region (str): The region for which the data is generated.
        report_type (str): The type of report to generate data for.
        start_date (datetime): The start date of the report period.
        end_date (datetime): The end date of the report period.

    Returns:
        List[ReportData]: A list of ReportData objects containing the generated data.
    """
    entities = {
        'Ad Campaigns': ['Product Campaign', 'Brand Campaign', 'Display Campaign'],
        'Ad Groups': ['Ad Group'],
        'Product Ads': ['Product Ad'],
        'Keywords': ['Keyword'],
        'Product Targets': ['Product Targeting', 'Contextual Targeting', 'Audience Targeting']
    }
    days = (end_date - start_date).days
    data = []
    
    for day in range(days):
        for _ in range(random.randint(5, 10)):  # Random number of entities per day
            sales = random.uniform(1000, 5000)
            spend = random.uniform(100, 500)
            clicks = random.randint(50, 200)
            impressions = random.randint(1000, 10000)
            orders = random.randint(10, 50)
            acos = spend / sales if sales != 0 else 0
            roas = sales / spend if spend != 0 else 0
            cvr = orders / clicks if clicks != 0 else 0
            ctr = clicks / impressions if impressions != 0 else 0

            data.append(ReportData(
                region=region,
                id=str(uuid4()),
                name=f'{report_type} Item {random.randint(1, 100)}',
                entity=random.choice(entities[report_type]),
                date=(start_date + timedelta(days=day)).strftime('%Y-%m-%d'),
                sales=sales,
                spend=spend,
                clicks=clicks,
                impressions=impressions,
                orders=orders,
                acos=acos,
                roas=roas,
                cvr=cvr,
                ctr=ctr
            ))
    return data


@app.post('/create_report')
def create_report(request: ReportRequest):
    """
    Creates a new report based on the provided request.

    Args:
        request (ReportRequest): The report request containing report type and date range.

    Returns:
        dict: A dictionary containing the generated report ID.

    Raises:
        HTTPException: If an invalid report type is provided.
    """
    if request.report_type not in REPORT_TYPES:
        raise HTTPException(status_code=400, detail='Invalid report type')
    
    report_id = str(uuid4())
    ready_time = datetime.now() + timedelta(seconds=random.randint(1, 5))  # Random time to be ready
    
    # Insert the report into the database
    cursor.execute('''
    INSERT INTO reports (id, region, report_type, start_date, end_date, status, ready_time, data)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (report_id, request.region, request.report_type, request.start_date.isoformat(), request.end_date.isoformat(), 'pending', ready_time.isoformat(), None))
    conn.commit()
    
    return {'report_id': report_id}


@app.get('/report_status/{report_id}')
def report_status(report_id: str):
    """
    Retrieves the status of a report.

    Args:
        report_id (str): The ID of the report to check.

    Returns:
        dict: A dictionary containing the current status of the report.

    Raises:
        HTTPException: If the report is not found.
    """
    cursor.execute('SELECT status, ready_time FROM reports WHERE id = ?', (report_id,))
    report = cursor.fetchone()
    if not report:
        raise HTTPException(status_code=404, detail='Report not found')
    
    status, ready_time_str = report
    ready_time = datetime.fromisoformat(ready_time_str)
    
    # Update report status based on the current time
    if datetime.now() >= ready_time:
        status = 'ready'
        cursor.execute('UPDATE reports SET status = ? WHERE id = ?', (status, report_id))
        conn.commit()
    
    return {'status': status}


@app.get('/download_report/{report_id}')
def download_report(report_id: str):
    """
    Downloads a report if it's ready.

    Args:
        report_id (str): The ID of the report to download.

    Returns:
        dict: A dictionary containing the report data.

    Raises:
        HTTPException: If the report is not found or not ready.
    """
    cursor.execute('SELECT status, region, report_type, start_date, end_date, data FROM reports WHERE id = ?', (report_id,))
    report = cursor.fetchone()
    if not report:
        raise HTTPException(status_code=404, detail='Report not found')
    
    status, region, report_type, start_date_str, end_date_str, data = report
    if status != 'ready':
        raise HTTPException(status_code=400, detail='Report not ready')
    
    # Generate report data if not already generated
    if data is None:
        report_data = generate_report_data(
            region,
            report_type,
            datetime.fromisoformat(start_date_str),
            datetime.fromisoformat(end_date_str)
        )
        data = [rd.dict() for rd in report_data]  # Convert to list of dictionaries
        cursor.execute('UPDATE reports SET data = ? WHERE id = ?', (str(data), report_id))
        conn.commit()
    else:
        data = eval(data)  # Convert the string back to list of dictionaries
    
    return {'data': data}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8000)


