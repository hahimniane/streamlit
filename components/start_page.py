"""
Start Page Component - SAWGraph Landing Page
Displays logo, project description, and link to project website
"""
import streamlit as st
import os

def render_start_page(project_dir: str):
    """
    Render the start/landing page with SAWGraph logo and project information.
    
    Args:
        project_dir: Path to project directory for locating assets
    """
    # Try to load logo - check multiple possible locations and filenames
    possible_logo_paths = [
        os.path.join(project_dir, "assets", "Sawgraph-Logo-transparent.png"),
        os.path.join(project_dir, "assets", "sawgraph_logo.png"),
        os.path.join(project_dir, "assets", "logo.png"),
    ]
    
    logo_path = None
    for path in possible_logo_paths:
        if os.path.exists(path):
            logo_path = path
            break
    
    # Center the content
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown("<br>", unsafe_allow_html=True)

        # Display logo if it exists, otherwise show placeholder
        # Logo displayed on white background as requested
        if logo_path:
            st.markdown("""
            <div style='background-color: white; padding: 20px; border-radius: 10px; text-align: center;'>
            """, unsafe_allow_html=True)
            st.image(logo_path, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("Logo will be displayed here once the file is added to `assets/Sawgraph-Logo-transparent.png`")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Project description
        st.markdown("""
        <div style='text-align: center; padding: 20px;'>
            <h2>Welcome to SAWGraph PFAS Explorer</h2>
            <p style='font-size: 1.1em; line-height: 1.6;'>
                This app is developed as part of the project 
                <strong>"Safe Agricultural Products and Water Graph (SAWGraph): 
                An Open Knowledge Network to Monitor and Trace PFAS and Other Contaminants 
                in the Nation's Food and Water Systems"</strong>.
            </p>
            <p style='font-size: 1em; margin-top: 20px;'>
                <a href='https://sawgraph.github.io' target='_blank' style='color: #1f77b4; text-decoration: none;'>
                    Learn more about the project → sawgraph.github.io
                </a>
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("<br><br>", unsafe_allow_html=True)
        
        # Instructions
        st.markdown("""
        ### Getting Started
        
        1. **Select an Analysis Type** from the sidebar to begin
        2. **Choose a Geographic Region** (State → County → Subdivision)
        3. **Configure Analysis Parameters** as needed
        4. **Execute the Query** to view results
        
        Available analyses include:
        - 🌊 **PFAS Upstream Tracing**: Trace potential contamination sources upstream
        - ⬇️ **PFAS Downstream Tracing**: Explore downstream transport and impacts
        - 🏭 **Samples Near Facilities**: Find PFAS samples near industrial facilities
        """)
        
        st.markdown("<br>", unsafe_allow_html=True)
