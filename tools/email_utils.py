# tools/email_utils.py
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

FROM_EMAIL = os.getenv("EMAIL_FROM", "Compliance Updates <noreply@compliance.franklinbutler.com>")

def send_article_email(to_email: str, article: dict) -> bool:
    title = article.get("title") or "Untitled"
    link = article.get("link") or "#"
    summary = article.get("ai_summary") or article.get("summary") or article.get("content") or ""

    html_content = f"""
    <div style="font-family:system-ui,-apple-system,sans-serif;max-width:600px;margin:auto;">
      <h2 style="color:#004b50;">{title}</h2>
      <p>{summary}</p>
      <p><a href="{link}" target="_blank" rel="noopener">Read the full article</a></p>
      <hr>
      <p style="font-size:0.85rem;color:#777;">Sent via compliance.franklinbutler.com</p>
    </div>
    """

    try:
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        message = Mail(
            from_email=FROM_EMAIL,
            to_emails=to_email,
            subject=f"Shared article: {title}",
            html_content=html_content,
        )
        resp = sg.send(message)
        return 200 <= resp.status_code < 300
    except Exception as e:
        print("SendGrid error:", e)
        return False
