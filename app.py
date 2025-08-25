# app.py
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# --- Page Configuration (MUST be the first Streamlit command) ---
st.set_page_config(
    page_title="FIFA 23 Player Dashboard",
    page_icon="⚽",
    layout="wide"
)

# --- Database Connection ---
@st.cache_resource
def init_connection():
    """Connects to the native PostgreSQL database using the superuser."""
    try:
        conn = psycopg2.connect(
            host="localhost", port="5432", database="fifadb",
            user="postgres", password="postgres"
        )
        return conn
    except psycopg2.OperationalError:
        st.error("Could not connect to the PostgreSQL database. Please make sure the service is running.")
        return None

@st.cache_data
def fetch_data(_conn):
    """Fetches all data from the master_player_table."""
    if _conn is None:
        return pd.DataFrame()
    df = pd.read_sql("SELECT * FROM master_player_table;", _conn)
    return df

# --- Main App ---
st.title("⚽ FIFA 23 Ultimate Player Dashboard")
st.markdown("An interactive dashboard to explore the FIFA 23 player dataset from your PostgreSQL database.")

# Connect and fetch data
connection = init_connection()
player_data = fetch_data(connection)

if not player_data.empty:
    
    # --- Sidebar Filters ---
    st.sidebar.header("Filter Players")
    
    # Filter by Nationality
    nationalities = ["All"] + sorted(player_data['nationality'].unique())
    selected_nationality = st.sidebar.selectbox("Nationality", nationalities)
    
    # Filter by Overall Rating
    min_overall, max_overall = int(player_data['overall'].min()), int(player_data['overall'].max())
    selected_overall = st.sidebar.slider(
        "Overall Rating", 
        min_value=min_overall, 
        max_value=max_overall, 
        value=(min_overall, max_overall)
    )

    # Apply filters
    filtered_df = player_data[
        (player_data['overall'] >= selected_overall[0]) &
        (player_data['overall'] <= selected_overall[1])
    ]
    if selected_nationality != "All":
        filtered_df = filtered_df[filtered_df['nationality'] == selected_nationality]

    # --- Top Metrics ---
    st.subheader("Dashboard Summary")
    
    total_players = len(filtered_df)
    avg_overall = filtered_df['overall'].mean()
    top_player = filtered_df.loc[filtered_df['value_eur'].idxmax()]

    col1, col2, col3 = st.columns(3)
    col1.metric("Players Displayed", f"{total_players:,}")
    col2.metric("Average Overall", f"{avg_overall:.1f}")
    col3.metric("Most Valuable Player", top_player['short_name'], f"€{top_player['value_eur']:,.0f}")
    
    st.markdown("---")

    # --- Chart Tabs ---
    tab1, tab2, tab3, tab4 = st.tabs(["Player Attributes", "Value Analysis", "Geographic Distribution", "Raw Data"])

    with tab1:
        st.header("Player Attribute Distributions")
        col_attr1, col_attr2 = st.columns(2)
        
        with col_attr1:
            # CHART 1: Age Distribution
            st.write("#### Player Age Distribution")
            fig_age = px.histogram(filtered_df, x="age", nbins=25, title="Count of Players by Age")
            st.plotly_chart(fig_age, use_container_width=True)

        with col_attr2:
            # CHART 2: Overall Rating Distribution
            st.write("#### Player Overall Rating Distribution")
            fig_overall = px.histogram(filtered_df, x="overall", nbins=25, title="Count of Players by Overall Rating")
            st.plotly_chart(fig_overall, use_container_width=True)

        # CHART 3: Preferred Foot Distribution
        st.write("#### Preferred Foot")
        foot_counts = filtered_df['preferred_foot'].value_counts()
        fig_pie = px.pie(values=foot_counts.values, names=foot_counts.index, title="Player Preferred Foot")
        st.plotly_chart(fig_pie, use_container_width=True)


    with tab2:
        st.header("Player Value and Potential Analysis")
        
        # CHART 4: Overall vs. Value
        st.write("#### Overall Rating vs. Market Value")
        fig_scatter_value = px.scatter(
            filtered_df.sample(n=min(1500, len(filtered_df))), # Use a sample
            x="overall", y="value_eur", 
            hover_name="short_name", title="Overall vs. Value (in Euros)",
            labels={"value_eur": "Market Value (€)", "overall": "Overall Rating"}
        )
        st.plotly_chart(fig_scatter_value, use_container_width=True)

        # CHART 5: Age vs. Potential
        st.write("#### Age vs. Potential")
        fig_scatter_potential = px.scatter(
            filtered_df.sample(n=min(1500, len(filtered_df))), # Use a sample
            x="age", y="potential", 
            hover_name="short_name", title="Age vs. Potential Rating",
            labels={"age": "Age", "potential": "Potential Rating"}
        )
        st.plotly_chart(fig_scatter_potential, use_container_width=True)
        
    with tab3:
        st.header("Geographic and Club Analysis")

        # CHART 6: World Map of Player Nationalities
        st.write("#### World Map of Player Nationalities")
        country_counts = filtered_df['nationality'].value_counts().reset_index()
        country_counts.columns = ['nationality', 'count']
        fig_map = px.choropleth(
            country_counts,
            locations="nationality",
            locationmode="country names",
            color="count",
            hover_name="nationality",
            color_continuous_scale=px.colors.sequential.Plasma,
            title="Number of Players by Country"
        )
        st.plotly_chart(fig_map, use_container_width=True)

        # CHART 7: Top 15 Clubs by Average Rating
        st.write("#### Top 15 Clubs by Average Player Rating")
        avg_rating_by_club = filtered_df.groupby('club_name')['overall'].mean().nlargest(15).sort_values(ascending=False)
        st.dataframe(avg_rating_by_club)


    with tab4:
        # CHART 8: Raw Data Table
        st.header("Player Data Explorer")
        st.dataframe(filtered_df)
else:
    st.warning("Could not load data from the database. Please ensure the load script has been run.")