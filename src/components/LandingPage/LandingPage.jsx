import React from 'react'
import { useNavigate } from 'react-router-dom'
import './LandingPage.css'
import { RightOutlined } from '@ant-design/icons'

const LandingPage = () => {
  const navigate = useNavigate();
  return (
    <div className='lp-div'>
      <div className='lp-intro-div'>
        <h1 className='lp-intro-heading'>A Simple<br/>Pay-Per-View<br/>for DAO- Events</h1>
        <img src="/assets/images/man-laptop.png" alt="man-laptop.png" className='lp-intro-img' />
      </div>
      <div
        className="lp-btn-div"
        onClick={() => navigate('/events')}
      >
        <span className="lp-di-text">Get Started</span>
        <RightOutlined className="lp-di-icon" />
      </div>
    </div>
  )
}

export default LandingPage