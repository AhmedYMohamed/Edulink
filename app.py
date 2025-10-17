import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_option_menu import option_menu
from streamlit_extras.stylable_container import stylable_container

st.set_page_config(page_title="Teachme", layout="wide")

# ---- SIDEBAR NAV ----
with st.sidebar:
    choice = option_menu(
        "Teachme",
        ["Home", "Students", "Teachers", "Documents", "Events"],
        icons=["house", "people", "person-badge", "folder", "calendar-event"],
        menu_icon="mortarboard",
        default_index=1,
    )

# ---- HEADER / PROFILE CARD ----
col1, col2 = st.columns([1.2, 2.2], gap="large")

with col1:
    with stylable_container(
        key="profile_card",
        css_styles="""
            {background-color: var(--secondary-background-color);
             padding: 16px; border-radius: 16px;}
            img {border-radius: 16px;}
        """,
    ):
        st.image("https://picsum.photos/seed/student/540/320", use_column_width=True)
        st.markdown("### Cooper Levin")
        st.caption("Advantage Art School, 10")
        st.write("**DOB:** 09.01.2008  \n**Email:** CooperLevin@gmail.com  \n**Phone:** +5541266542")
        c1, c2 = st.columns(2)
        c1.button("Send Message")
        c2.button("Connect")

with col2:
    with stylable_container("progress_card",
        css_styles="{background-color:var(--secondary-background-color);padding:16px;border-radius:16px;}"):
        st.markdown("#### Student’s progress")
        df = pd.DataFrame({
            "month": ["Sep","Oct","Nov","Dec","Jan","Feb","Mar","Apr","May"],
            "score": [45,30,60,35,20,50,15,30,33],
        })
        fig = px.line(df, x="month", y="score", markers=True)
        st.plotly_chart(fig, use_container_width=True)

# ---- TASKS / SCHEDULE / PEOPLE ----
cA, cB, cC = st.columns([1.2, 1.2, 1], gap="large")

with cA:
    with stylable_container("tasks",
        css_styles="{background-color:var(--secondary-background-color);padding:16px;border-radius:16px;}"):
        st.markdown("#### Tasks")
        st.markdown("- Test **Geometric Progression** — *until 22.06.2022*")
        st.markdown("- Worksheet **Series & Sequences** — *until 24.06.2022*")

with cB:
    with stylable_container("schedule",
        css_styles="{background-color:var(--secondary-background-color);padding:16px;border-radius:16px;}"):
        st.markdown("#### Schedule")
        sched = pd.DataFrame({
            "time": ["08:00–09:00","09:15–10:15","10:30–11:30","11:45–12:30"],
            "subject": ["Maths","History","Maths","History"],
            "room": [505,403,505,403]
        })
        st.dataframe(sched, use_container_width=True, hide_index=True)

with cC:
    with stylable_container("teachers",
        css_styles="{background-color:var(--secondary-background-color);padding:16px;border-radius:16px;}"):
        st.markdown("#### Teachers")
        st.markdown("• **Jenny Wilson** — Math")
        st.markdown("• **Kristin Watson** — History")
        st.markdown("• **Annette Black** — Armenian")

# ---- RATING / ATTENDANCE CHARTS ----
c1, c2 = st.columns(2, gap="large")
for title, col in [("Rating", c1), ("Attendance", c2)]:
    with col:
        with stylable_container(title.lower(),
            css_styles="{background-color:var(--secondary-background-color);padding:16px;border-radius:16px;}"):
            st.markdown(f"#### {title}")
            bars = pd.DataFrame({"Subject":["Math","History","Math","Math","Math"], "Value":[80,25,60,40,90]})
            st.plotly_chart(px.bar(bars, x="Subject", y="Value"), use_container_width=True)
