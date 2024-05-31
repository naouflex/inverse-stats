import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# Parameters 
n_values = 10

dbr_reserve_param = [1000000 * (1.5 ** i) for i in range(n_values) if 1000000 * (1.5 ** i) < 5000000] #(start = 1M, end = 5M, step = *1.5)
dola_reserve_param = [100000 * (1.5 ** i) for i in range(n_values) if 100000 * (1.5 ** i) < 1000000] # (start = 100K, end = 1M, step = *1.5)
dbr_rate_per_year_param = [1000000 * (1.5 ** i) for i in range(n_values) if 1000000 * (1.5 ** i) < 5000000] # (start = 1M, end = 5M, step = *1.5)
dola_in_param = [1000 * (1.5 ** i) for i in range(n_values) if 1000 * (1.5 ** i) < 20000]


total_days_param = 7

time_elapsed_range = [60 * 60 * 24 * days for days in range(1, total_days_param+1)]  # 1 to 7 days in seconds

# DataFrame to store results
results = []

# Looping through each combination of parameters
for dbr_reserve_param in (dbr_reserve_param):
    for dola_reserve in (dola_reserve_param):
        for dbr_rate_per_year in (dbr_rate_per_year_param):
            for time_elapsed in time_elapsed_range:
                k = dbr_reserve_param * dola_reserve

                dbr_rate_per_second = dbr_rate_per_year / 365 / 24 / 60 / 60

                dbrs_in = time_elapsed * dbr_rate_per_second
                dbr_reserve = dbr_reserve_param + dbrs_in
                dola_reserve = k / dbr_reserve

                for exact_dola_in in dola_in_param:
                    dbr_out = (exact_dola_in * dbr_reserve) / (dola_reserve + exact_dola_in)
                    
                    # Storing results
                    results.append({
                        "dbr_reserve": float(dbr_reserve),
                        "dola_reserve": float(dola_reserve),
                        "dbr_rate_per_year": float(dbr_rate_per_year),
                        "dbrs_in": float(dbrs_in),
                        "time_elapsed_days": time_elapsed / (60 * 60 * 24),  # Convert seconds back to days for readability
                        "dbr_out": float(dbr_out),
                        "dola_in": float(exact_dola_in),
                        "dbr_price": float(exact_dola_in / dbr_out)
                    })

# Creating DataFrame
df = pd.DataFrame(results)

# Modified function to create 3D scatter plot and add to a subplot
def create_3d_scatter(fig, row, col, x, y, z, color, color_scale, title, x_title, y_title, z_title):
    scatter = go.Scatter3d(
        x=x, y=y, z=z, mode='markers',
        marker=dict(size=5, color=color, colorscale=color_scale, opacity=0.8)
    )
    fig.add_trace(scatter, row=row, col=col)
    fig.update_scenes(dict(
        xaxis_title=x_title, 
        yaxis_title=y_title, 
        zaxis_title=z_title), 
        row=row, col=col)
    fig.update_layout(title_text=title, title_x=0.5)

# Creating a 3x3 subplot figure
fig = make_subplots(
    rows=3, cols=3,
    subplot_titles=(
        "DBR Reserve, DOLA Reserve, and DBR Price over Time",
        "DBR Rate per Year, DOLA In, and DBR Price over Time",
        "DBR Reserve, DBR Rate per Year, and DBR Price over Time",
        "DOLA Reserve, DOLA In, and DBR Price over Time",
        "DBR Output vs. DBR Reserve and DBR Rate Per Year",
        "DOLA In vs. DBR Reserve and DOLA Reserve",
        "DBR Price vs. Time Elapsed and DOLA In",
        "DBR Reserve vs. DOLA Reserve and Time Elapsed"
    ),
    specs=[[{'type': 'scatter3d'} for _ in range(3)] for _ in range(3)]
)

# Adding each plot to the subplot figure
# Plot 1
create_3d_scatter(fig, 1, 1, df['time_elapsed_days'], df['dbr_reserve'], df['dbr_price'],
                  df['dola_reserve'], 'Plasma', '', 
                  'Time Elapsed (Days)', 'DBR Reserve', 'DBR Price')

# Plot 2
create_3d_scatter(fig, 1, 2, df['time_elapsed_days'], df['dbr_rate_per_year'], df['dbr_price'],
                  df['dola_in'], 'Plasma', '', 
                  'Time Elapsed (Days)', 'DBR Rate per Year', 'DBR Price')

# Plot 3
create_3d_scatter(fig, 1, 3, df['time_elapsed_days'], df['dbr_reserve'], df['dbr_price'],
                  df['dbr_rate_per_year'], 'Plasma', '', 
                  'Time Elapsed (Days)', 'DBR Reserve', 'DBR Price')

# Plot 4
create_3d_scatter(fig, 2, 1, df['time_elapsed_days'], df['dola_reserve'], df['dbr_price'],
                  df['dola_in'], 'Plasma', '', 
                  'Time Elapsed (Days)', 'DOLA Reserve', 'DBR Price')

# Plot 5
create_3d_scatter(fig, 2, 2, df['dbr_reserve'], df['dbr_rate_per_year'], df['dbr_out'],
                  df['time_elapsed_days'], 'Plasma', '', 
                  'DBR Reserve', 'DBR Rate per Year', 'DBR Output')

# Plot 6
create_3d_scatter(fig, 2, 3, df['dbr_reserve'], df['dola_reserve'], df['dola_in'],
                  df['time_elapsed_days'], 'Plasma', '', 
                  'DBR Reserve', 'DOLA Reserve', 'DOLA In')

# Plot 7
create_3d_scatter(fig, 3, 1, df['time_elapsed_days'], df['dola_in'], df['dbr_price'],
                  df['dbr_reserve'], 'Plasma', '', 
                  'Time Elapsed (Days)', 'DOLA In', 'DBR Price')

# Plot 8
create_3d_scatter(fig, 3, 2, df['dbr_reserve'], df['dola_reserve'], df['time_elapsed_days'],
                  df['dbr_out'], 'Plasma', '', 
                  'DBR Reserve', 'DOLA Reserve', 'Time Elapsed (Days)')

# Adjust layout and display the figure
fig.update_layout(height=1800, width=1800, showlegend=False)
fig.show()