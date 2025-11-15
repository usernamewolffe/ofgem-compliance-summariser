# summariser/views.py
from django.shortcuts import render
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from .model import Entry  # note: singular file in your app

def summaries(request):
    q = request.GET.get("q", "").strip()
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    sources = request.GET.getlist("sources")

    qs = Entry.objects.all()

    if q:
        qs = qs.filter(title__icontains=q) | qs.filter(content__icontains=q)

    if date_from:
        qs = qs.filter(published_at__date__gte=date_from)
    if date_to:
        qs = qs.filter(published_at__date__lte=date_to)

    if sources:
        qs = qs.filter(source__in=sources)

    # --- Pagination ---
    page = request.GET.get("page", 1)
    per_page = int(request.GET.get("per_page", 25))  # tweak as you like (10/25/50)
    paginator = Paginator(qs, per_page)
    try:
        page_obj = paginator.page(page)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    # derive sources (for the checkbox list)
    all_sources = list(
        Entry.objects.values_list("source", flat=True).distinct().order_by("source")
    )

    context = {
        "entries": page_obj.object_list,
        "page_obj": page_obj,
        "paginator": paginator,
        "active": {
            "q": q,
            "date_from": date_from,
            "date_to": date_to,
            "sources": sources,
            "per_page": per_page,
        },
        "all_sources": all_sources,
    }
    return render(request, "summariser/summaries.html", context)
