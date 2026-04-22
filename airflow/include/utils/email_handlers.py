# airflow/dags/include/utils/email_handlers.py

from airflow.utils.email import send_email
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

EMAIL = ['iamomowale@outlook.com']

def notify_success(context):
    """
    Callback function to send a success email when a DAG completes.
    """
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context.get('logical_date')
    
    subject = f"DAG Success: {dag_id}"
    body = f"""
    <h3>DAG Success Notification</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Run ID:</b> {run_id}</p>
    <p><b>Finished At:</b> {execution_date}</p>
    <br>
    <p>The pipeline has completed all stages successfully.</p>
    """
    
    send_email(
        to=EMAIL,
        subject=subject,
        html_content=body   
    )


# def notify_failure(context):
#    error_msg = context.get('exception', 'No specific exception recorded')
#    
#    send_email(
#        to=EMAIL,
#        subject="Supplychain360 DAG failed",
#        html_content=f"""
        # DAG <b>{context['dag'].dag_id}</b> failed.<br>
        # Task: {context['task_instance'].task_id if context.get('task_instance') else 'N/A'}<br>
        # Run ID: {context['run_id']}<br>
        # <pre>{error_msg}</pre>
#        """
#    )